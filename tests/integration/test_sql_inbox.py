"""The inbox adapter on PostgreSQL 16 and MySQL 8.0: migration parity, one transaction for
effect plus inbox row, the unique-key race (C0b-17), and the version guard under a race (K3)."""

from __future__ import annotations

import threading
import time

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, inspect, insert, select

from factbus.adapters import applied_versions_table, inbox_table
from factbus.domain import ConsumedEvent, InboxOutcome, SubjectVersion

HOLD_SECONDS = 1.5


def _event(event_id: str) -> ConsumedEvent:
    return ConsumedEvent(event_id=event_id, envelope={"event_id": event_id}, message_id=f"m-{event_id}", receive_count=1)


@pytest.fixture
def effects(inbox_world):
    engine = inbox_world[0]
    table = Table(f"fx_{inbox_world[2].name}", MetaData(), Column("id", Integer, primary_key=True, autoincrement=True),
                  Column("event_id", String(128), nullable=False))
    table.create(engine)
    yield table
    table.drop(engine)


def _count(engine, table) -> int:
    with engine.connect() as connection:
        return len(connection.execute(select(table)).all())


def _writer(table, *, hold: threading.Event | None = None, reached: threading.Event | None = None):
    def effect(connection, event):
        connection.execute(insert(table).values(event_id=event.event_id))
        if reached is not None:
            reached.set()
        if hold is not None:
            assert hold.wait(30), "worker A was never released"
    return effect


def test_migration_matches_the_runtime_tables(inbox_world):
    engine, _, inbox, versions = inbox_world
    inspector = inspect(engine)
    for runtime in (inbox, versions):
        reflected = {c["name"]: c for c in inspector.get_columns(runtime.name)}
        assert set(reflected) == {c.name for c in runtime.columns}
        for column in runtime.columns:
            assert reflected[column.name]["nullable"] == column.nullable, column.name
        assert inspector.get_pk_constraint(runtime.name)["constrained_columns"] == [
            c.name for c in runtime.primary_key.columns
        ]
    reflected_indexes = {tuple(i["column_names"]) for i in inspector.get_indexes(inbox.name)}
    assert ("received_at",) in reflected_indexes  # inbox migration 0002, used by purge
    assert inbox_table(MetaData()).name == "factbus_inbox"
    assert applied_versions_table(MetaData()).name == "factbus_applied_version"


def test_applied_then_duplicate(inbox_world, effects):
    engine, sql_inbox, _, _ = inbox_world
    assert sql_inbox.process(_event("e1"), subject=None, effect=_writer(effects)) is InboxOutcome.APPLIED
    assert sql_inbox.process(_event("e1"), subject=None, effect=_writer(effects)) is InboxOutcome.DUPLICATE
    assert _count(engine, effects) == 1 and sql_inbox.contains("e1")


def test_a_failed_effect_rolls_back_the_inbox_row(inbox_world, effects):
    engine, sql_inbox, _, _ = inbox_world

    def failing(connection, event):
        connection.execute(insert(effects).values(event_id=event.event_id))
        raise RuntimeError("provider down after the effect row was written")

    with pytest.raises(RuntimeError):
        sql_inbox.process(_event("e1"), subject=SubjectVersion("s", 1), effect=failing)
    assert _count(engine, effects) == 0
    assert not sql_inbox.contains("e1") and sql_inbox.applied_version("s") is None
    assert sql_inbox.process(_event("e1"), subject=SubjectVersion("s", 1), effect=_writer(effects)) is InboxOutcome.APPLIED


def test_version_guard_drops_at_or_below_and_commits_the_inbox_row(inbox_world, effects):
    engine, sql_inbox, _, _ = inbox_world
    assert sql_inbox.process(_event("v3"), subject=SubjectVersion("s", 3), effect=_writer(effects)) is InboxOutcome.APPLIED
    assert sql_inbox.process(_event("v2"), subject=SubjectVersion("s", 2), effect=_writer(effects)) is InboxOutcome.STALE
    assert sql_inbox.process(_event("v3b"), subject=SubjectVersion("s", 3), effect=_writer(effects)) is InboxOutcome.STALE
    assert sql_inbox.process(_event("v2"), subject=SubjectVersion("s", 2), effect=_writer(effects)) is InboxOutcome.DUPLICATE
    assert sql_inbox.process(_event("v4"), subject=SubjectVersion("s", 4), effect=_writer(effects)) is InboxOutcome.APPLIED
    assert sql_inbox.applied_version("s") == 4 and _count(engine, effects) == 2


def _race(sql_inbox, effects, first, second):
    """Worker A holds its transaction open after the inbox insert and the effect row;
    worker B handles its event inside that window. Returns both outcomes, the inbox
    reads B made, and how long B waited."""
    reached, hold = threading.Event(), threading.Event()
    seen_calls: list[tuple[str, bool]] = []
    original_seen = sql_inbox._seen

    def recording_seen(connection, event_id):
        answer = original_seen(connection, event_id)
        seen_calls.append((threading.current_thread().name, answer))
        return answer

    sql_inbox._seen = recording_seen
    results: dict[str, object] = {}
    a = threading.Thread(
        name="A",
        target=lambda: results.setdefault("a", sql_inbox.process(first[0], subject=first[1],
                                                                 effect=_writer(effects, hold=hold, reached=reached))),
    )
    a.start()
    assert reached.wait(30), "worker A never reached the barrier: the fault was not reached"

    def run_b():
        started = time.monotonic()
        results["b"] = sql_inbox.process(second[0], subject=second[1], effect=_writer(effects))
        results["b_waited"] = time.monotonic() - started

    b = threading.Thread(name="B", target=run_b)
    b.start()
    time.sleep(HOLD_SECONDS)
    assert b.is_alive(), "worker B finished while A held its transaction: B never contended"
    hold.set()
    a.join(30)
    b.join(30)
    sql_inbox._seen = original_seen
    return results, [answer for name, answer in seen_calls if name == "B"]


def test_c0b_17_two_workers_one_event_id_give_one_effect_and_no_failure(inbox_world, effects):
    engine, sql_inbox, _, _ = inbox_world
    results, b_reads = _race(sql_inbox, effects, (_event("race"), None), (_event("race"), None))
    assert results["a"] is InboxOutcome.APPLIED
    assert results["b"] is InboxOutcome.DUPLICATE  # a success, not a failure
    assert b_reads == [False]  # B's read missed A's uncommitted row: the unique key decided
    assert results["b_waited"] >= HOLD_SECONDS
    assert _count(engine, effects) == 1


def test_k3_newer_first_insert_wins_a_version_race(inbox_world, effects):
    engine, sql_inbox, _, _ = inbox_world
    results, _ = _race(sql_inbox, effects, (_event("v6"), SubjectVersion("s", 6)), (_event("v5"), SubjectVersion("s", 5)))
    assert results["a"] is InboxOutcome.APPLIED and results["b"] is InboxOutcome.STALE
    assert sql_inbox.applied_version("s") == 6 and _count(engine, effects) == 1


def test_k3_older_first_insert_is_superseded_in_a_version_race(inbox_world, effects):
    engine, sql_inbox, _, _ = inbox_world
    results, _ = _race(sql_inbox, effects, (_event("v5"), SubjectVersion("s", 5)), (_event("v6"), SubjectVersion("s", 6)))
    assert results["a"] is InboxOutcome.APPLIED and results["b"] is InboxOutcome.APPLIED
    assert sql_inbox.applied_version("s") == 6 and _count(engine, effects) == 2
