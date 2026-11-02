"""`factbus.once` against every claim store: the same suite, the same semantics.

DynamoDB runs under moto's `mock_aws`, which honours condition expressions, so the
conditional writes that carry the guarantee are exercised rather than stubbed.
`SqlClaimStore` runs on in-memory SQLite (its `ON CONFLICT DO NOTHING` path); the
PostgreSQL and MySQL paths, and concurrency, are proved in tests/integration.
"""

from __future__ import annotations

from datetime import timedelta

import boto3
import pytest
from moto import mock_aws
from sqlalchemy import MetaData, create_engine
from sqlalchemy.pool import StaticPool

from factbus.adapters.dynamodb_claims import DynamoClaimStore
from factbus.adapters.memory import MemoryClaimStore
from factbus.adapters.sql_claims import SqlClaimStore, claims_table
from factbus.domain.claims import Claim, ClaimState
from factbus.once import CertainlyNotSent, KeyReused, StillPending, Unknown, once
from tests.support import T0, ManualClock

TABLE = "claims"
KEY = "org-1#welcome#u-42"


@pytest.fixture(params=["memory", "dynamodb", "sql"])
def store(request):
    if request.param == "memory":
        yield MemoryClaimStore()
        return
    if request.param == "sql":
        engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
        table = claims_table(MetaData(), TABLE)
        table.create(engine)
        yield SqlClaimStore(engine, table)
        engine.dispose()
        return
    with mock_aws():
        client = boto3.client("dynamodb", region_name="eu-west-1")
        client.create_table(
            TableName=TABLE,
            # The consumer's inbox table: claims share it with inbox and version items.
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                                  {"AttributeName": "sk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield DynamoClaimStore(client, TABLE)


@pytest.fixture
def clock():
    return ManualClock()


class Recorder:
    """A side effect that counts its calls and does whatever the test scripts next."""

    def __init__(self, *outcomes) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0

    def __call__(self, to: str) -> dict:
        self.calls += 1
        outcome = self.outcomes.pop(0) if self.outcomes else {"message_id": f"m-{self.calls}", "to": to}
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def guard(store, clock, effect, **options):
    return once(lambda to: KEY, store, clock=clock, digest=lambda to: f"sha-{to}", **options)(effect)


def other_holder(deadline_seconds: float = 300) -> Claim:
    return Claim(
        key=KEY,
        state=ClaimState.PENDING,
        owner="someone-else",
        deadline=T0 + timedelta(seconds=deadline_seconds),
        digest="sha-a@x",
        expires=T0 + timedelta(days=14),
    )


def test_duplicate_after_success_returns_the_recorded_result_without_calling(store, clock):
    effect = Recorder()
    send = guard(store, clock, effect)

    first = send("a@x")
    second = send("a@x")

    assert first == second == {"message_id": "m-1", "to": "a@x"}
    assert effect.calls == 1
    assert store.read(KEY).state is ClaimState.DONE


def test_a_call_that_raises_is_unknown_and_never_repeated(store, clock):
    effect = Recorder(RuntimeError("socket closed after the request was written"))
    send = guard(store, clock, effect)

    with pytest.raises(Unknown) as raised:
        send("a@x")
    assert isinstance(raised.value.__cause__, RuntimeError)
    with pytest.raises(Unknown):
        send("a@x")

    assert effect.calls == 1
    assert store.read(KEY).state is ClaimState.UNKNOWN


def test_certainly_not_sent_releases_the_claim_and_a_retry_calls_again(store, clock):
    effect = Recorder(CertainlyNotSent("throttled before acceptance"))
    send = guard(store, clock, effect)

    with pytest.raises(CertainlyNotSent):
        send("a@x")
    assert store.read(KEY) is None
    result = send("a@x")

    assert effect.calls == 2
    assert result == {"message_id": "m-2", "to": "a@x"}
    assert store.read(KEY).state is ClaimState.DONE


def test_a_fresh_claim_held_elsewhere_is_still_pending(store, clock):
    assert store.claim(other_holder(), T0)
    effect = Recorder()
    clock.advance(299)

    with pytest.raises(StillPending):
        guard(store, clock, effect)("a@x")

    assert effect.calls == 0
    assert store.read(KEY).state is ClaimState.PENDING


def test_a_stale_claim_resolves_to_unknown_without_calling(store, clock):
    assert store.claim(other_holder(), T0)
    effect = Recorder()
    send = guard(store, clock, effect)
    clock.advance(301)

    with pytest.raises(Unknown):
        send("a@x")
    with pytest.raises(Unknown):
        send("a@x")

    assert effect.calls == 0
    row = store.read(KEY)
    assert (row.state, row.owner) == (ClaimState.UNKNOWN, "someone-else")


def test_the_same_key_with_different_content_is_refused(store, clock):
    effect = Recorder()
    send = guard(store, clock, effect)
    send("a@x")

    with pytest.raises(KeyReused):
        send("b@x")

    assert effect.calls == 1


def test_a_late_writer_cannot_overwrite_another_outcome(store, clock):
    effect = Recorder()
    guard(store, clock, effect)("a@x")
    holder = store.read(KEY).owner

    assert not store.settle(KEY, "someone-else", ClaimState.UNKNOWN)
    assert not store.release(KEY, "someone-else")
    # Even the owner cannot move a row that already left `pending`.
    assert not store.settle(KEY, holder, ClaimState.UNKNOWN)
    assert not store.release(KEY, holder)
    assert store.read(KEY).state is ClaimState.DONE


def test_a_holder_that_outlives_its_deadline_cannot_overwrite_unknown(store, clock):
    """The real race: the call runs past `claim_seconds`, a redelivery marks the claim
    `unknown`, then the original call returns. Its settle loses; the row stays `unknown`."""
    late = []

    def slow_send(to: str) -> dict:
        clock.advance(301)
        with pytest.raises(Unknown):
            send(to)  # the redelivery, arriving while this call is still running
        late.append(to)
        return {"message_id": "m-late"}

    send = guard(store, clock, slow_send)

    assert send("a@x") == {"message_id": "m-late"}
    assert late == ["a@x"]
    assert store.read(KEY).state is ClaimState.UNKNOWN


def test_a_row_past_retention_no_longer_guards_the_key(store, clock):
    effect = Recorder()
    send = guard(store, clock, effect, retention=timedelta(days=1))
    send("a@x")
    clock.advance(timedelta(days=1, seconds=2).total_seconds())

    send("a@x")

    assert effect.calls == 2


def test_options_that_cannot_hold_the_guarantee_are_refused(store):
    with pytest.raises(ValueError):
        once(KEY, store, claim_seconds=0)
    with pytest.raises(ValueError):
        once(KEY, store, claim_seconds=600, retention=timedelta(seconds=600))


def test_claims_share_the_inbox_table_without_colliding(monkeypatch, clock):
    """One consumer table holds inbox, version and claim items: an event id used as a
    `once` key does not read as an inbox record, and the reverse."""
    from factbus.adapters.dynamodb_inbox import DynamoInbox
    from factbus.domain import ConsumedEvent, InboxOutcome

    for name, value in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                        "AWS_REGION": "eu-west-1", "FACTBUS_INBOX_TABLE": TABLE}.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("FACTBUS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    with mock_aws():
        boto3.client("dynamodb", region_name="eu-west-1").create_table(
            TableName=TABLE,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                                  {"AttributeName": "sk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        inbox, claims = DynamoInbox.from_env(clock=clock), DynamoClaimStore.from_env()
        calls = []

        @once(key=lambda event_id: event_id, store=claims, clock=clock)
        def send(event_id):
            calls.append(event_id)
            return "sent"

        assert send("ev-1") == "sent"
        consumed = ConsumedEvent(event_id="ev-1", envelope={"id": "ev-1"}, message_id="m-1", receive_count=1)
        assert inbox.process(consumed, subject=None, effect=lambda tx, e: None) is InboxOutcome.APPLIED
        assert inbox.process(consumed, subject=None, effect=lambda tx, e: None) is InboxOutcome.DUPLICATE
        assert send("ev-1") == "sent" and calls == ["ev-1"]
        assert claims.read("ev-1").key == "ev-1"
