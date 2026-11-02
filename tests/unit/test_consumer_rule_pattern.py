"""The consumer module's `detail_pattern`, matched by an EventBridge model.

`iac/modules/consumer/tests/consumer.tftest.hcl` proves the module renders exactly the
fixture's `pattern` from `detail_pattern = { channel = ["whatsapp"] }`. Here a rule with
that pattern runs under moto: the whatsapp event reaches the rule's queue and the email
event does not. tests/integration asks LocalStack's `TestEventPattern` the same
question. Both are emulators, so this proves the pattern's shape, not AWS itself."""

from __future__ import annotations

import json
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

FIXTURE = Path(__file__).resolve().parents[2] / "iac/modules/consumer/tests/fixtures/detail_pattern.json"
REGION = "eu-west-1"


def load_fixture() -> dict:
    return json.loads(FIXTURE.read_text())


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        yield boto3.client("events", region_name=REGION), boto3.client("sqs", region_name=REGION)


def delivered_channels(aws, pattern: dict) -> list[str]:
    """Put both fixture events through a rule with `pattern`; return the channels its queue received."""
    events, sqs = aws
    fixture = load_fixture()
    queue_url = sqs.create_queue(QueueName="rule-probe")["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    events.put_rule(Name="probe", EventPattern=json.dumps(pattern))
    events.put_targets(Rule="probe", Targets=[{"Id": "q", "Arn": arn}])
    entries = [
        {"Source": event["source"], "DetailType": event["detail-type"], "Detail": json.dumps(event["detail"])}
        for event in (fixture["matching_event"], fixture["non_matching_event"])
    ]
    assert events.put_events(Entries=entries)["FailedEntryCount"] == 0
    messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10).get("Messages", [])
    return sorted(json.loads(message["Body"])["detail"]["channel"] for message in messages)


def test_the_detail_filter_admits_the_matching_channel_only(aws):
    assert delivered_channels(aws, load_fixture()["pattern"]) == ["whatsapp"]


def test_without_the_detail_filter_both_channels_are_delivered(aws):
    """Control: the default pattern (no `detail`) admits both, so the refusal above is the filter's."""
    default = {key: value for key, value in load_fixture()["pattern"].items() if key != "detail"}
    assert delivered_channels(aws, default) == ["email", "whatsapp"]
