"""`SqlProducer` on SQLite: a staged row is published after commit, never after rollback,
never from a savepoint that rolled back, and a missed row goes out on the sweep.
PostgreSQL and MySQL locking is proved in tests/integration."""

from __future__ import annotations

import json

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, event, select
from sqlalchemy.orm import Session

from factbus.envelope import Event
from factbus.sql_producer import SqlProducer
from tests.support import CountingMetrics, ManualClock, ScriptedBus

def sqlite_engine(path):
    """SQLite with real SAVEPOINT support: pysqlite's own transaction handling breaks
    `begin_nested`, so hand BEGIN to SQLAlchemy (the documented pysqlite recipe)."""
    engine = create_engine(f"sqlite:///{path}")

    @event.listens_for(engine, "connect")
    def _connect(dbapi_connection, _record):
        dbapi_connection.isolation_level = None

    @event.listens_for(engine, "begin")
    def _begin(connection):
        connection.exec_driver_sql("BEGIN")

    return engine


orders = Table("orders", MetaData(), Column("id", Integer, primary_key=True), Column("item", String(50)))


@pytest.fixture
def world(tmp_path):
    engine = sqlite_engine(tmp_path / 'p.db')
    clock, bus = ManualClock(), ScriptedBus()
    producer = SqlProducer(engine=engine, bus=bus, metrics=CountingMetrics(), clock=clock)
    producer.table.metadata.create_all(engine)
    orders.metadata.create_all(engine)
    producer.attach()
    yield engine, producer, bus, clock
    producer.detach()
    engine.dispose()


def placed(n: int) -> Event:
    return Event(id=f"ord-{n}", type="order.placed", source="shop.orders", subject=str(n), data={"n": n})


def sent_ids(bus: ScriptedBus) -> list[str]:
    return [json.loads(entry.detail)["id"] for call in bus.calls for entry in call]


def test_a_committed_row_is_published_after_commit(world):
    engine, producer, bus, _ = world
    with Session(engine) as session, session.begin():
        session.execute(orders.insert().values(id=1, item="book"))
        producer.stage(session, placed(1))
        assert bus.calls == []  # nothing leaves before commit
    assert sent_ids(bus) == ["ord-1"]
    row = producer.store.row(1)
    assert row is not None and row.sent_at is not None


def test_an_undated_dict_envelope_is_stamped_at_staging(world):
    """Consumers refuse an envelope with no timestamp of its own (review finding, PR #10),
    so `stage` dates a dict envelope that has none, and leaves a dated one alone."""
    engine, producer, bus, clock = world
    legacy = {"event_id": "legacy-1", "type": "order.placed", "producer": "shop.orders", "data": {}}
    cloud = {"specversion": "1.0", "id": "ce-1", "type": "order.placed", "source": "shop.orders", "data": {}}
    dated = {**legacy, "event_id": "legacy-2", "occurred_at": "2026-01-01T00:00:00+00:00"}
    with Session(engine) as session, session.begin():
        for envelope in (legacy, cloud, dated):
            producer.stage(session, envelope)

    details = [json.loads(entry.detail) for call in bus.calls for entry in call]
    assert [d.get("occurred_at") or d.get("time") for d in details] == [
        clock().isoformat(), clock().isoformat(), "2026-01-01T00:00:00+00:00",
    ]
    assert "time" not in details[0] and "occurred_at" not in details[1]
    assert "occurred_at" not in legacy  # the caller's dict is not mutated


def test_a_rolled_back_transaction_publishes_nothing_and_leaves_no_row(world):
    engine, producer, bus, _ = world
    with pytest.raises(RuntimeError), Session(engine) as session, session.begin():
        producer.stage(session, placed(1))
        raise RuntimeError("business write failed")
    assert bus.calls == []
    with engine.connect() as connection:
        assert connection.execute(select(producer.table)).first() is None


def test_a_row_staged_in_a_rolled_back_savepoint_is_not_published(world):
    engine, producer, bus, _ = world
    with Session(engine) as session, session.begin():
        producer.stage(session, placed(1))
        nested = session.begin_nested()
        producer.stage(session, placed(2))
        nested.rollback()
    assert sent_ids(bus) == ["ord-1"]


def test_a_row_staged_in_a_released_savepoint_is_published_on_the_outer_commit(world):
    """SQLAlchemy 1.4 fires `after_commit` when a savepoint is RELEASED, while the outer
    transaction is still open. The staged row must survive that and go out on the real
    commit, not wait for the sweep."""
    engine, producer, bus, _ = world
    with Session(engine) as session, session.begin():
        with session.begin_nested():
            producer.stage(session, placed(1))
        assert bus.calls == []  # released, not committed
        session.execute(orders.insert().values(id=1, item="book"))
    assert sent_ids(bus) == ["ord-1"]


def test_a_session_without_a_default_bind_never_fails_its_commit(world):
    """Per-table binds and no default bind: `get_bind()` raises inside the listener. The
    write committed, so `commit()` must not report a failure; the sweep sends the row."""
    engine, producer, bus, _ = world
    with Session(binds={orders: engine, producer.table: engine}) as session:
        session.execute(orders.insert().values(id=1, item="book"))
        producer.stage(session, placed(1))
        session.commit()
    assert bus.calls == []
    with engine.connect() as connection:
        assert connection.execute(select(orders)).first() is not None
        assert connection.execute(select(producer.table)).first() is not None


def test_a_bus_failure_never_reaches_the_caller_and_the_sweep_sends_it(world):
    engine, producer, bus, clock = world
    bus.raise_code = "InternalException"
    with Session(engine) as session, session.begin():
        producer.stage(session, placed(1))
    assert producer.store.row(1).sent_at is None  # committed, unsent
    bus.raise_code = None
    clock.advance(31)
    report = producer.sweep()
    assert report["sent"] == 1
    assert sent_ids(bus)[-1] == "ord-1"


def test_publish_on_commit_off_still_stages_for_the_sweep(tmp_path):
    engine = sqlite_engine(tmp_path / 'q.db')
    clock, bus = ManualClock(), ScriptedBus()
    producer = SqlProducer(engine=engine, bus=bus, metrics=CountingMetrics(), clock=clock, publish_on_commit=False)
    producer.table.metadata.create_all(engine)
    producer.attach()
    try:
        with Session(engine) as session, session.begin():
            producer.stage(session, placed(1))
        assert bus.calls == []
        clock.advance(31)
        assert producer.sweep()["sent"] == 1
    finally:
        producer.detach()


def test_a_session_on_another_engine_is_ignored(world, tmp_path):
    _, producer, bus, _ = world
    other = sqlite_engine(tmp_path / 'other.db')
    producer.table.metadata.create_all(other)
    with Session(other) as session, session.begin():
        producer.store.add(session, placed(9).to_envelope())
        session.info.setdefault(producer._staged_key, [])  # an unrelated session's info
    assert bus.calls == []


def test_a_producer_on_another_engine_leaves_this_producers_rows(world, tmp_path):
    # Both producers use the default table name, so both listen for the same staged
    # key. The one attached first must not take the other's rows.
    _, first, first_bus, _ = world
    engine = sqlite_engine(tmp_path / 'second.db')
    clock, bus = ManualClock(), ScriptedBus()
    second = SqlProducer(engine=engine, bus=bus, metrics=CountingMetrics(), clock=clock)
    second.table.metadata.create_all(engine)
    second.attach()
    try:
        with Session(engine) as session, session.begin():
            second.stage(session, placed(1))
        assert first_bus.calls == []
        assert sent_ids(bus) == ["ord-1"]
    finally:
        second.detach()
        engine.dispose()


def test_attach_is_idempotent(world):
    engine, producer, bus, _ = world
    producer.attach()
    with Session(engine) as session, session.begin():
        producer.stage(session, placed(1))
    assert sent_ids(bus) == ["ord-1"]  # published once, not once per attach


def test_connect_requires_a_region_and_reads_no_environment(monkeypatch, tmp_path):
    """The `us-east-1` default and the environment fallback are gone: a region in the
    process environment does not stand in for the argument."""
    import factbus.sql_producer as module

    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
    engine = sqlite_engine(tmp_path / "connect.db")
    with pytest.raises(TypeError, match="region_name"):
        SqlProducer.connect(engine, bus_name="orders")
    with pytest.raises(ValueError, match="region_name"):
        SqlProducer.connect(engine, bus_name="orders", region_name="")

    built = []
    monkeypatch.setattr(module, "build_eventbridge_bus", lambda **kwargs: built.append(kwargs) or ScriptedBus())
    producer = SqlProducer.connect(engine, bus_name="orders", region_name="ap-south-1", endpoint_url="http://localhost:4576")
    assert isinstance(producer, SqlProducer)
    assert built == [{"bus_name": "orders", "region_name": "ap-south-1", "endpoint_url": "http://localhost:4576"}]
