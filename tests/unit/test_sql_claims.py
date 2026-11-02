"""`SqlClaimStore` and `SqlInbox.purge` on in-memory SQLite: what the shared `once` suite
does not cover (purge, limits, construction). PostgreSQL and MySQL, and every race, are
in tests/integration."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import MetaData, create_engine, select
from sqlalchemy.pool import StaticPool

from factbus.adapters import SqlClaimStore, SqlInbox, applied_versions_table, claims_table, inbox_table
from factbus.domain import ConsumedEvent, InboxOutcome
from factbus.domain.claims import Claim, ClaimState
from factbus.once import once
from tests.support import T0, ManualClock


@pytest.fixture
def engine():
    eng = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    yield eng
    eng.dispose()


@pytest.fixture
def clock():
    return ManualClock()


@pytest.fixture
def claims(engine, clock):
    table = claims_table(MetaData())
    table.create(engine)
    return SqlClaimStore(engine, table, clock=clock)


def _claim(key: str, *, retention_days: float = 35) -> Claim:
    return Claim(
        key=key,
        state=ClaimState.PENDING,
        owner=f"owner-{key}",
        deadline=T0 + timedelta(minutes=5),
        digest=None,
        expires=T0 + timedelta(days=retention_days),
    )


def _keys(store: SqlClaimStore) -> set[str]:
    with store._engine.connect() as connection:
        return {row.claim_key for row in connection.execute(select(store.table.c.claim_key))}


def test_the_default_table_is_named_and_built_from_a_name(engine):
    assert claims_table(MetaData()).name == "factbus_claims"
    assert SqlClaimStore(engine).table.name == "factbus_claims"
    assert SqlClaimStore(engine, "my_claims").table.name == "my_claims"


def test_purge_deletes_only_rows_past_the_cutoff_in_batches(claims, clock):
    for n in range(5):
        assert claims.claim(_claim(f"old-{n}"), T0)
    clock.advance(timedelta(days=36).total_seconds())
    assert claims.claim(_claim("young"), clock())

    assert claims.purge(batch_size=2) == 5

    assert _keys(claims) == {"young"}
    assert claims.purge() == 0


def test_purge_keeps_a_row_whose_own_retention_has_not_run(claims, clock):
    """A `once` kept longer than the purge window keeps its guard."""
    assert claims.claim(_claim("long", retention_days=60), T0)
    clock.advance(timedelta(days=36).total_seconds())

    assert claims.purge() == 0
    assert _keys(claims) == {"long"}


def test_a_purged_key_is_free_again(claims, clock):
    calls = []
    send = once("k", claims, clock=clock)(lambda: calls.append(1) or "sent")
    send()
    clock.advance(timedelta(days=36).total_seconds())
    assert claims.purge() == 1

    send()

    assert calls == [1, 1]


def test_purge_arguments_are_checked(claims):
    with pytest.raises(ValueError):
        claims.purge(timedelta(0))
    with pytest.raises(ValueError):
        claims.purge(batch_size=0)


def test_keys_and_digests_longer_than_the_columns_are_refused(claims):
    with pytest.raises(ValueError):
        claims.claim(_claim("k" * 256), T0)
    with pytest.raises(ValueError):
        claims.claim(Claim(key="k", state=ClaimState.PENDING, owner="o", deadline=T0, digest="d" * 256,
                           expires=T0 + timedelta(days=1)), T0)
    assert claims.claim(_claim("k" * 255), T0)


def test_an_unsupported_dialect_is_refused():
    class Fake:
        class dialect:  # mirrors Engine.dialect
            name = "oracle"

    with pytest.raises(ValueError):
        SqlClaimStore(Fake())  # type: ignore[arg-type]


def test_inbox_purge_deletes_only_rows_past_the_cutoff(engine, clock):
    metadata = MetaData()
    inbox_rows, versions = inbox_table(metadata), applied_versions_table(metadata)
    metadata.create_all(engine)
    inbox = SqlInbox(engine, inbox_rows, versions, clock)

    def event(event_id):
        return ConsumedEvent(event_id=event_id, envelope={}, message_id=event_id, receive_count=1)

    for n in range(5):
        assert inbox.process(event(f"old-{n}"), subject=None, effect=lambda c, e: None) is InboxOutcome.APPLIED
    clock.advance(timedelta(days=35, seconds=1).total_seconds())
    assert inbox.process(event("young"), subject=None, effect=lambda c, e: None) is InboxOutcome.APPLIED

    assert inbox.purge(batch_size=2) == 5

    assert not inbox.contains("old-0") and inbox.contains("young")
    assert inbox.process(event("young"), subject=None, effect=lambda c, e: None) is InboxOutcome.DUPLICATE
    with pytest.raises(ValueError):
        inbox.purge(batch_size=0)
