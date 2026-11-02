"""`SqlClaimStore` behind `once` on PostgreSQL 16 and MySQL 8.0: migration parity, the
claim race, a crash after the claim, a released claim, and purge (alone and while
consumption runs). Each engine runs its own insert path: `ON CONFLICT DO NOTHING` on
PostgreSQL, `ON DUPLICATE KEY UPDATE` with an owner read-back on MySQL."""

from __future__ import annotations

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import MetaData, inspect, select

from factbus.adapters import (
    SqlClaimStore,
    SqlInbox,
    claims_table,
    downgrade_claims_0001,
    upgrade_claims_0001,
)
from factbus.domain import ConsumedEvent, InboxOutcome
from factbus.domain.claims import Claim, ClaimState
from factbus.once import CertainlyNotSent, KeyReused, StillPending, Unknown, once
from tests.support import T0, ManualClock

KEY = "tenant-1#welcome#user-42"
WORKERS = 8


@pytest.fixture
def claims_world(engine):
    """A fresh claim table created by claims migration 0001, dropped by its downgrade."""
    name = f"cl_{uuid.uuid4().hex[:10]}"
    with engine.begin() as connection:
        upgrade_claims_0001(Operations(MigrationContext.configure(connection)), name)
    clock = ManualClock()
    table = claims_table(MetaData(), name)
    yield engine, SqlClaimStore(engine, table, clock=clock), table, clock
    with engine.begin() as connection:
        downgrade_claims_0001(Operations(MigrationContext.configure(connection)), name)


class Crash(BaseException):
    """Stands in for the process dying mid-call: `once` sees no exception it can handle,
    so nothing after the claim runs, exactly as if the host had gone away."""


def _guard(store, clock, effect, **options):
    return once(lambda to: KEY, store, clock=clock, digest=lambda to: f"sha-{to}", **options)(effect)


def _keys(engine, table) -> set[str]:
    with engine.connect() as connection:
        return {row.claim_key for row in connection.execute(select(table.c.claim_key))}


def test_migration_matches_the_runtime_table(claims_world):
    engine, _, table, _ = claims_world
    inspector = inspect(engine)
    reflected = {c["name"]: c for c in inspector.get_columns(table.name)}
    assert set(reflected) == {c.name for c in table.columns}
    for column in table.columns:
        assert reflected[column.name]["nullable"] == column.nullable, column.name
    assert inspector.get_pk_constraint(table.name)["constrained_columns"] == ["claim_key"]
    indexes = {tuple(i["column_names"]) for i in inspector.get_indexes(table.name) if not i.get("unique")}
    assert indexes == {tuple(c.name for c in index.columns) for index in table.indexes} == {("created_at",)}
    assert claims_table(MetaData()).name == "factbus_claims"


def test_timestamps_keep_microseconds(claims_world):
    _, store, _, _ = claims_world
    deadline = T0 + timedelta(seconds=300, microseconds=123456)
    claim = Claim(key=KEY, state=ClaimState.PENDING, owner="o", deadline=deadline, digest=None,
                  expires=T0 + timedelta(days=35, microseconds=654321))
    assert store.claim(claim, T0)
    row = store.read(KEY)
    assert (row.deadline, row.expires) == (claim.deadline, claim.expires)


def test_concurrent_claims_of_one_key_have_exactly_one_winner(claims_world):
    _, store, _, _ = claims_world
    start = threading.Barrier(WORKERS)

    def attempt(n: int) -> bool:
        claim = Claim(key=KEY, state=ClaimState.PENDING, owner=f"owner-{n}", deadline=T0 + timedelta(minutes=5),
                      digest="d", expires=T0 + timedelta(days=35))
        start.wait(10)
        return store.claim(claim, T0)

    with ThreadPoolExecutor(WORKERS) as pool:
        won = list(pool.map(attempt, range(WORKERS)))

    assert won.count(True) == 1
    assert store.read(KEY).owner == f"owner-{won.index(True)}"


def test_concurrent_once_calls_run_the_effect_once_and_the_rest_are_still_pending(claims_world):
    _, store, _, clock = claims_world
    start, others_done = threading.Barrier(WORKERS), threading.Event()
    calls = []

    def effect(to: str) -> dict:
        calls.append(to)
        assert others_done.wait(30), "the other attempts never finished"
        return {"message_id": "m-1"}

    send = _guard(store, clock, effect)
    outcomes: list[object] = []
    lock = threading.Lock()

    def attempt(_: int) -> None:
        start.wait(10)
        try:
            result: object = send("a@x")
        except StillPending as error:
            result = error
        with lock:
            outcomes.append(result)
            if sum(isinstance(o, StillPending) for o in outcomes) == WORKERS - 1:
                others_done.set()

    with ThreadPoolExecutor(WORKERS) as pool:
        list(pool.map(attempt, range(WORKERS)))

    assert len(calls) == 1
    assert sum(isinstance(o, StillPending) for o in outcomes) == WORKERS - 1
    assert [o for o in outcomes if not isinstance(o, StillPending)] == [{"message_id": "m-1"}]
    assert store.read(KEY).state is ClaimState.DONE
    assert send("a@x") == {"message_id": "m-1"} and len(calls) == 1


def test_a_crash_after_the_claim_resolves_to_unknown_and_never_calls_again(claims_world):
    _, store, _, clock = claims_world
    calls = []

    def crashing_send(to: str) -> dict:
        calls.append(to)
        raise Crash

    with pytest.raises(Crash):
        _guard(store, clock, crashing_send)("a@x")
    assert store.read(KEY).state is ClaimState.PENDING

    recovered = _guard(store, clock, lambda to: calls.append(to) or {"message_id": "again"})
    with pytest.raises(StillPending):
        recovered("a@x")  # inside the claim's deadline the holder may still be alive
    clock.advance(301)
    with pytest.raises(Unknown):
        recovered("a@x")
    with pytest.raises(Unknown):
        recovered("a@x")

    assert calls == ["a@x"]
    assert store.read(KEY).state is ClaimState.UNKNOWN


def test_certainly_not_sent_releases_the_claim_and_the_retry_calls(claims_world):
    _, store, _, clock = claims_world
    outcomes = [CertainlyNotSent("throttled before acceptance")]
    calls = []

    def send_once(to: str) -> dict:
        calls.append(to)
        if outcomes:
            raise outcomes.pop(0)
        return {"message_id": f"m-{len(calls)}"}

    send = _guard(store, clock, send_once)
    with pytest.raises(CertainlyNotSent):
        send("a@x")
    assert store.read(KEY) is None

    assert send("a@x") == {"message_id": "m-2"}
    assert send("a@x") == {"message_id": "m-2"}
    assert len(calls) == 2


def test_the_same_key_with_a_different_digest_is_refused(claims_world):
    _, store, _, clock = claims_world
    calls = []
    send = _guard(store, clock, lambda to: calls.append(to) or "ok")
    send("a@x")
    with pytest.raises(KeyReused):
        send("b@x")
    assert calls == ["a@x"]


def test_keys_that_differ_only_in_case_are_separate_claims(claims_world):
    """MySQL's default collation ignores case and `utf8mb4_bin` ignores trailing spaces;
    claim keys must do neither (review findings, PR #10)."""
    engine, store, table, clock = claims_world
    calls = []

    @once(lambda key: key, store, clock=clock)
    def send(key: str) -> str:
        calls.append(key)
        return f"sent-{key}"

    keys = ["tenant-A#user", "tenant-a#user", "tenant-a#user "]
    assert [send(key) for key in keys] == [f"sent-{key}" for key in keys]
    assert calls == keys
    assert _keys(engine, table) == set(keys)


def test_settle_and_release_are_conditional_on_the_holder(claims_world):
    _, store, _, clock = claims_world
    _guard(store, clock, lambda to: "ok")("a@x")
    holder = store.read(KEY).owner
    assert not store.settle(KEY, "someone-else", ClaimState.UNKNOWN)
    assert not store.release(KEY, "someone-else")
    assert not store.settle(KEY, holder, ClaimState.UNKNOWN)
    assert not store.release(KEY, holder)
    assert store.read(KEY).state is ClaimState.DONE


def test_an_expired_row_is_taken_over_by_the_next_claim(claims_world):
    _, store, _, clock = claims_world
    calls = []
    send = _guard(store, clock, lambda to: calls.append(to) or f"m-{len(calls)}", retention=timedelta(days=1))
    assert send("a@x") == "m-1"
    clock.advance(timedelta(days=1, seconds=1).total_seconds())

    assert send("a@x") == "m-2"
    assert len(calls) == 2


def test_purge_deletes_old_rows_in_batches_and_keeps_young_ones(claims_world):
    engine, store, table, clock = claims_world
    for n in range(7):
        assert once(f"old-{n}", store, clock=clock)(lambda: "ok")() == "ok"
    clock.advance(timedelta(days=36).total_seconds())
    once("young", store, clock=clock)(lambda: "ok")()

    assert store.purge(batch_size=3) == 7
    assert _keys(engine, table) == {"young"}


def test_a_purge_during_claims_does_not_break_dedupe_for_young_keys(claims_world):
    engine, store, table, clock = claims_world
    for n in range(300):
        once(f"old-{n}", store, clock=clock)(lambda: "ok")()
    clock.advance(timedelta(days=36).total_seconds())
    calls: list[str] = []
    lock = threading.Lock()

    def effect(key: str) -> str:
        with lock:
            calls.append(key)
        return key

    send = once(lambda key: key, store, clock=clock)(effect)
    purged: list[int] = []
    purger = threading.Thread(target=lambda: purged.append(store.purge(batch_size=25)))
    purger.start()
    for n in range(40):
        assert send(f"young-{n}") == f"young-{n}"
        assert send(f"young-{n}") == f"young-{n}"  # the duplicate is answered from the row
    purger.join(60)

    assert purged == [300]
    assert sorted(calls) == sorted(f"young-{n}" for n in range(40))
    assert _keys(engine, table) == {f"young-{n}" for n in range(40)}


def test_an_inbox_purge_during_consumption_does_not_break_dedupe(inbox_world):
    """Old rows go; every event inside the age limit is applied once while the purge runs."""
    engine, _, inbox_rows, versions = inbox_world
    clock = ManualClock()
    inbox = SqlInbox(engine, inbox_rows, versions, clock)

    def event(event_id: str) -> ConsumedEvent:
        return ConsumedEvent(event_id=event_id, envelope={}, message_id=event_id, receive_count=1)

    for n in range(300):
        assert inbox.process(event(f"old-{n}"), subject=None, effect=lambda c, e: None) is InboxOutcome.APPLIED
    clock.advance(timedelta(days=36).total_seconds())
    applied: list[str] = []
    purged: list[int] = []
    purger = threading.Thread(target=lambda: purged.append(inbox.purge(batch_size=25)))
    purger.start()
    outcomes = []
    for n in range(40):
        for _ in range(2):
            outcomes.append(inbox.process(event(f"young-{n}"), subject=None,
                                          effect=lambda c, e: applied.append(e.event_id)))
    purger.join(60)

    assert purged == [300]
    assert outcomes.count(InboxOutcome.APPLIED) == 40 and outcomes.count(InboxOutcome.DUPLICATE) == 40
    assert sorted(applied) == sorted(f"young-{n}" for n in range(40))
    with engine.connect() as connection:
        left = {row.event_id for row in connection.execute(select(inbox_rows.c.event_id))}
    assert left == {f"young-{n}" for n in range(40)}
