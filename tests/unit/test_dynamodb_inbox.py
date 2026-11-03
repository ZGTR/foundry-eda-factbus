"""The DynamoDB inbox against moto: one TransactWriteItems for inbox item, version guard
and the effect's own items; DUPLICATE and STALE found by the pre-read and by a cancelled
transaction; the 100-item limit; and the existing `Consumer` driving it end to end."""

from __future__ import annotations

import subprocess
import sys
from datetime import timedelta

import boto3
import botocore.exceptions
import pytest
from moto import mock_aws

from factbus.adapters import DYNAMODB_CLIENT_CONFIG, DynamoInbox, DynamoTransaction, TransactionTooLarge
from factbus.adapters.dynamodb_inbox import MAX_TRANSACT_ITEMS, build_dynamo_inbox
from factbus.application import Consumer
from factbus.domain import BackoffSchedule, ConsumedEvent, InboxOutcome, SubjectVersion
from factbus.envelope import Event
from tests.support import T0, CountingMetrics, ManualClock, ScriptedVisibility, sqs_record

TABLE = "factbus-inbox"
REGION = "us-east-1"


@pytest.fixture
def client(monkeypatch):
    for name, value in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                        "AWS_SESSION_TOKEN": "testing", "AWS_DEFAULT_REGION": REGION}.items():
        monkeypatch.setenv(name, value)
    with mock_aws():
        dynamodb = boto3.client("dynamodb", region_name=REGION, config=DYNAMODB_CLIENT_CONFIG)
        dynamodb.create_table(
            TableName=TABLE,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                                  {"AttributeName": "sk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield dynamodb


@pytest.fixture
def clock():
    return ManualClock()


@pytest.fixture
def inbox(client, clock):
    return DynamoInbox(client, TABLE, clock=clock)


def _event(event_id: str) -> ConsumedEvent:
    return ConsumedEvent(event_id=event_id, envelope={"id": event_id}, message_id=f"m-{event_id}", receive_count=1)


class Writer:
    """An effect that adds one item per event to the transaction, and counts its calls."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(self, tx: DynamoTransaction, event: ConsumedEvent) -> None:
        self.calls.append(event.event_id)
        tx.put(TABLE, {"pk": f"fx#{event.event_id}", "sk": "fx", "event_id": event.event_id})


def _effect_items(client) -> list[str]:
    items = client.scan(TableName=TABLE)["Items"]
    return sorted(item["event_id"]["S"] for item in items if item["pk"]["S"].startswith("fx#"))


def _item_count(client) -> int:
    return client.scan(TableName=TABLE, Select="COUNT")["Count"]


def test_applied_once_records_the_inbox_item_with_its_ttl(client, inbox):
    writer = Writer()
    assert inbox.process(_event("e1"), subject=None, effect=writer) is InboxOutcome.APPLIED
    assert writer.calls == ["e1"] and _effect_items(client) == ["e1"] and inbox.contains("e1")
    item = client.get_item(TableName=TABLE, Key={"pk": {"S": "inbox#e1"}, "sk": {"S": "inbox"}})["Item"]
    assert int(item["expires_at"]["N"]) == int((T0 + timedelta(days=35)).timestamp())
    assert item["received_at"]["S"] == T0.isoformat()


def test_redelivery_is_duplicate_and_writes_no_second_effect_item(client, inbox):
    writer = Writer()
    assert inbox.process(_event("e1"), subject=None, effect=writer) is InboxOutcome.APPLIED
    assert inbox.process(_event("e1"), subject=None, effect=writer) is InboxOutcome.DUPLICATE
    assert writer.calls == ["e1"]  # the pre-read answered; the effect was not called again
    assert _effect_items(client) == ["e1"] and _item_count(client) == 2


def test_a_copy_recorded_after_the_read_cancels_the_transaction_as_duplicate(client, inbox):
    """C0b-17: another worker commits the same event_id between the read and the write;
    the inbox condition, not the read, decides."""

    def racing(tx, event):
        client.put_item(TableName=TABLE, Item={"pk": {"S": f"inbox#{event.event_id}"}, "sk": {"S": "inbox"}})
        tx.put(TABLE, {"pk": f"fx#{event.event_id}", "sk": "fx", "event_id": event.event_id})

    assert inbox.process(_event("race"), subject=None, effect=racing) is InboxOutcome.DUPLICATE
    assert _effect_items(client) == [] and _item_count(client) == 1


def test_an_effect_that_raises_writes_nothing_and_propagates(client, inbox):
    def failing(tx, event):
        tx.put(TABLE, {"pk": f"fx#{event.event_id}", "sk": "fx", "event_id": event.event_id})
        raise RuntimeError("provider down after the effect item was added")

    with pytest.raises(RuntimeError, match="provider down"):
        inbox.process(_event("e1"), subject=SubjectVersion("s", 1), effect=failing)
    assert _item_count(client) == 0
    assert not inbox.contains("e1") and inbox.applied_version("s") is None
    assert inbox.process(_event("e1"), subject=SubjectVersion("s", 1), effect=Writer()) is InboxOutcome.APPLIED


def test_an_effect_condition_failing_on_its_own_item_propagates_with_no_inbox_item(client, inbox):
    client.put_item(TableName=TABLE, Item={"pk": {"S": "widget#w1"}, "sk": {"S": "state"}})

    def create_widget(tx, event):
        tx.put(TABLE, {"pk": "widget#w1", "sk": "state", "by": event.event_id}, "attribute_not_exists(pk)")

    with pytest.raises(botocore.exceptions.ClientError) as raised:
        inbox.process(_event("e1"), subject=SubjectVersion("s", 1), effect=create_widget)
    assert raised.value.response["Error"]["Code"] == "TransactionCanceledException"
    assert not inbox.contains("e1") and inbox.applied_version("s") is None
    assert _item_count(client) == 1  # only the widget written before the event


def test_version_guard_newer_older_equal(client, inbox):
    writer = Writer()
    assert inbox.process(_event("v3"), subject=SubjectVersion("s", 3), effect=writer) is InboxOutcome.APPLIED
    assert inbox.process(_event("v2"), subject=SubjectVersion("s", 2), effect=writer) is InboxOutcome.STALE
    assert inbox.process(_event("v3b"), subject=SubjectVersion("s", 3), effect=writer) is InboxOutcome.STALE
    assert inbox.contains("v2") and inbox.contains("v3b")  # STALE records the inbox item
    assert inbox.process(_event("v2"), subject=SubjectVersion("s", 2), effect=writer) is InboxOutcome.DUPLICATE
    assert inbox.process(_event("v4"), subject=SubjectVersion("s", 4), effect=writer) is InboxOutcome.APPLIED
    assert writer.calls == ["v3", "v4"] and _effect_items(client) == ["v3", "v4"]
    assert inbox.applied_version("s") == 4


def test_a_newer_version_written_after_the_read_cancels_to_stale_with_the_inbox_recorded(client, inbox):
    """K3: a concurrent newer write for the subject wins; this event's effect items are
    dropped and only its inbox item is recorded."""
    assert inbox.process(_event("v1"), subject=SubjectVersion("s", 1), effect=Writer()) is InboxOutcome.APPLIED

    def racing(tx, event):
        client.put_item(TableName=TABLE, Item={"pk": {"S": "version#s"}, "sk": {"S": "version"}, "version": {"N": "9"}})
        tx.put(TABLE, {"pk": f"fx#{event.event_id}", "sk": "fx", "event_id": event.event_id})

    assert inbox.process(_event("v5"), subject=SubjectVersion("s", 5), effect=racing) is InboxOutcome.STALE
    assert inbox.contains("v5") and inbox.applied_version("s") == 9
    assert _effect_items(client) == ["v1"]


def test_the_transaction_refuses_the_101st_write_before_sending():
    tx = DynamoTransaction()
    for n in range(MAX_TRANSACT_ITEMS):
        tx.delete(TABLE, {"pk": f"k{n}", "sk": "x"})
    with pytest.raises(TransactionTooLarge, match="at most 100 writes"):
        tx.update(TABLE, {"pk": "one-too-many", "sk": "x"}, "SET n = :n", values={":n": 1})
    assert len(tx) == MAX_TRANSACT_ITEMS


def test_an_effect_past_the_limit_writes_nothing(client, inbox):
    def many(count):
        def effect(tx, event):
            for n in range(count):
                tx.put(TABLE, {"pk": f"fx#{event.event_id}-{n}", "sk": "fx", "event_id": f"{event.event_id}-{n}"})
        return effect

    with pytest.raises(TransactionTooLarge):
        inbox.process(_event("big"), subject=SubjectVersion("s", 1), effect=many(MAX_TRANSACT_ITEMS - 1))
    assert _item_count(client) == 0
    # Inbox Put + version Update + 98 effect items is exactly the limit, and commits.
    assert inbox.process(_event("big"), subject=SubjectVersion("s", 1),
                         effect=many(MAX_TRANSACT_ITEMS - 2)) is InboxOutcome.APPLIED
    assert len(_effect_items(client)) == MAX_TRANSACT_ITEMS - 2


def test_constructor_and_builder(client, clock):
    with pytest.raises(ValueError):
        DynamoInbox(client, "", clock=clock)
    with pytest.raises(ValueError):
        DynamoInbox(client, TABLE, retention=timedelta(0), clock=clock)
    built = build_dynamo_inbox(table_name=TABLE, region_name=REGION, clock=clock)
    assert built.table_name == TABLE
    assert built._client.meta.config.retries == {"total_max_attempts": 3, "mode": "standard"}


def _cloud_event(event_id: str, version: int, **data) -> dict:
    return Event(type="example.widget.changed", source="widgets", subject="w-1", id=event_id,
                 data=data, extensions={"authorityepoch": version}, time=T0.isoformat()).to_envelope()


def test_consumer_drives_the_dynamo_inbox_end_to_end(client, inbox, clock):
    def widget_effect(tx, event):
        envelope = event.envelope
        if envelope["data"].get("fail"):
            raise RuntimeError("provider refused")
        tx.put(TABLE, {"pk": f"fx#{event.event_id}", "sk": "fx", "event_id": event.event_id,
                       "subject": envelope["subject"], "version": envelope["authorityepoch"]})

    visibility, metrics = ScriptedVisibility(), CountingMetrics()
    consumer = Consumer(
        inbox=inbox, visibility=visibility, effect=widget_effect, schedule=BackoffSchedule(), clock=clock,
        metrics=metrics, version_of=lambda env: SubjectVersion(env["subject"], env["authorityepoch"]),
    )
    first = sqs_record(_cloud_event("evt-3", 3), message_id="m1", eventbridge_id="eb-original")
    replay = sqs_record(_cloud_event("evt-3", 3), message_id="m2", eventbridge_id="eb-replayed")
    older = sqs_record(_cloud_event("evt-2", 2), message_id="m3")
    failing = sqs_record(_cloud_event("evt-4", 4, fail=True), message_id="m4", receive_count=2)

    assert consumer.handle_batch([first, replay, older, failing]) == {"batchItemFailures": [{"itemIdentifier": "m4"}]}
    assert _effect_items(client) == ["evt-3"]
    assert inbox.contains("evt-2") and not inbox.contains("evt-4")
    assert inbox.applied_version("w-1") == 3
    assert (metrics.counts["ConsumerApplied"], metrics.counts["ConsumerDuplicates"],
            metrics.counts["ConsumerStale"]) == (1, 1, 1)
    assert visibility.calls == [("rh-m4-2", 120)]

    retried = sqs_record(_cloud_event("evt-4", 4), message_id="m4", receive_count=3)
    assert consumer.handle_batch([retried, older]) == {"batchItemFailures": []}
    assert _effect_items(client) == ["evt-3", "evt-4"] and inbox.applied_version("w-1") == 4


def test_importing_the_dynamodb_inbox_does_not_import_sqlalchemy():
    probe = (
        "import sys\n"
        "import factbus.adapters.dynamodb_inbox\n"
        "from factbus.adapters import DynamoInbox, DynamoTransaction\n"
        "assert 'sqlalchemy' not in sys.modules and 'alembic' not in sys.modules, 'eager SQL import'\n"
        "from factbus.adapters import SqlInbox\n"
        "assert 'sqlalchemy' in sys.modules, 'positive control: the lazy name did not load'\n"
    )
    result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr


def test_the_builder_takes_every_setting_as_an_argument(client, monkeypatch):
    """factbus reads no environment: with the region and table variables unset, the
    builder still builds on the named table from its arguments alone."""
    for name in ("FACTBUS_INBOX_TABLE", "AWS_REGION", "AWS_DEFAULT_REGION", "FACTBUS_ENDPOINT_URL", "AWS_ENDPOINT_URL"):
        monkeypatch.delenv(name, raising=False)
    built = build_dynamo_inbox(table_name=TABLE, region_name=REGION)
    assert built.table_name == TABLE
    assert built._client.meta.region_name == REGION
    writer = Writer()
    assert built.process(_event("arg-1"), subject=None, effect=writer) is InboxOutcome.APPLIED
    assert _effect_items(client) == ["arg-1"]


@pytest.mark.parametrize("missing", ["table_name", "region_name"])
def test_the_builder_refuses_a_missing_setting(missing):
    settings = {"table_name": TABLE, "region_name": REGION, missing: ""}
    with pytest.raises(ValueError, match=missing):
        build_dynamo_inbox(**settings)


def test_the_builder_passes_the_endpoint_through():
    built = build_dynamo_inbox(table_name=TABLE, region_name=REGION, endpoint_url="http://localhost:4576")
    assert built._client.meta.endpoint_url == "http://localhost:4576"


def test_an_effect_may_write_json_floats(client, inbox):
    """JSON numbers parse as floats, which boto3 refuses; the transaction stores them as
    exact decimals of their shortest repr."""

    def effect(tx, event):
        tx.put(TABLE, {"pk": "fx#f", "sk": "fx", "event_id": "f", "total": 19.99, "lines": [{"p": 0.1}]})

    assert inbox.process(_event("f"), subject=None, effect=effect) is InboxOutcome.APPLIED
    item = client.get_item(TableName=TABLE, Key={"pk": {"S": "fx#f"}, "sk": {"S": "fx"}})["Item"]
    assert item["total"] == {"N": "19.99"} and item["lines"]["L"][0]["M"]["p"] == {"N": "0.1"}
