"""The consumer module's `detail_pattern` against LocalStack 3.8's `TestEventPattern`.

The pattern is the shared fixture that `consumer.tftest.hcl` proves the module renders
from `detail_pattern = { channel = ["whatsapp"] }`. An emulator, so this proves the
pattern's shape, not AWS's matcher."""

from __future__ import annotations

import json
from pathlib import Path

import boto3

FIXTURE = Path(__file__).resolve().parents[2] / "iac/modules/consumer/tests/fixtures/detail_pattern.json"


def test_the_rule_for_whatsapp_refuses_an_email_event(localstack_endpoint):
    events = boto3.client("events", endpoint_url=localstack_endpoint, region_name="us-east-1",
                          aws_access_key_id="test", aws_secret_access_key="test")
    fixture = json.loads(FIXTURE.read_text())
    default = {key: value for key, value in fixture["pattern"].items() if key != "detail"}

    def matches(pattern: dict, event: dict) -> bool:
        return events.test_event_pattern(EventPattern=json.dumps(pattern), Event=json.dumps(event))["Result"]

    assert matches(fixture["pattern"], fixture["matching_event"]) is True
    assert matches(fixture["pattern"], fixture["non_matching_event"]) is False
    # Control: without the detail filter the email event matches, so the refusal is the filter's.
    assert matches(default, fixture["non_matching_event"]) is True
