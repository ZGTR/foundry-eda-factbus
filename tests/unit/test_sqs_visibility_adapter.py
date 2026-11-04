"""The SQS visibility adapter against botocore's Stubber: request shape, the error
mapping, and the written retry policy."""

from __future__ import annotations

import boto3
import pytest
from botocore.stub import Stubber

from factbus.adapters import SQS_CLIENT_CONFIG, SqsVisibility, build_sqs_visibility
from factbus.ports import VisibilityUnavailable

QUEUE = "https://sqs.us-east-1.amazonaws.com/000000000000/core-facts"


def _adapter():
    client = boto3.client("sqs", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="x",
                          config=SQS_CLIENT_CONFIG)
    return SqsVisibility(client, QUEUE), Stubber(client)


def test_change_visibility_names_queue_handle_and_seconds():
    adapter, stubber = _adapter()
    stubber.add_response(
        "change_message_visibility", {}, {"QueueUrl": QUEUE, "ReceiptHandle": "rh-1", "VisibilityTimeout": 600}
    )
    with stubber:
        adapter.change_visibility("rh-1", 600)
    stubber.assert_no_pending_responses()


def test_a_client_error_becomes_visibility_unavailable():
    adapter, stubber = _adapter()
    stubber.add_client_error("change_message_visibility", service_error_code="ReceiptHandleIsInvalid")
    with stubber, pytest.raises(VisibilityUnavailable):
        adapter.change_visibility("rh-stale", 30)


def test_client_is_built_with_an_explicit_two_attempt_policy():
    adapter = build_sqs_visibility(
        queue_url=QUEUE, region_name="us-east-1",
        session=boto3.session.Session(aws_access_key_id="x", aws_secret_access_key="x"),
    )
    retries = adapter._client.meta.config.retries
    assert retries["total_max_attempts"] == 2 and retries["mode"] == "standard"
    assert adapter.queue_url == QUEUE


def test_queue_url_is_required():
    with pytest.raises(ValueError):
        SqsVisibility(object(), "")


@pytest.mark.parametrize("region_name", [None, ""])
def test_region_is_required(monkeypatch, region_name):
    """F1 review round 3 (Low): a missing region must not fall back to AWS_DEFAULT_REGION."""
    from factbus.adapters.eventbridge import build_eventbridge_bus

    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-south-1")
    with pytest.raises(ValueError, match="region_name"):
        build_sqs_visibility(queue_url="https://sqs.us-east-1.amazonaws.com/1/q", region_name=region_name)
    with pytest.raises(ValueError, match="region_name"):
        build_eventbridge_bus(bus_name="b", region_name=region_name)
