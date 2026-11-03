"""`@consumer`: a Lambda handler that dedupes, filters by type and reports partial
batch failures, built lazily from the record's queue ARN."""

from __future__ import annotations

import pytest

from factbus.consumer_handler import consumer, queue_url_from_arn
from factbus.envelope import Event
from tests.support import CountingMetrics, MemoryInbox, ScriptedVisibility, sqs_record

ARN = "arn:aws:sqs:eu-west-1:123456789012:ledger"


def record(n: int, *, type_: str = "learning.byte.completed", message_id: str | None = None, eb: str = "eb-1") -> dict:
    envelope = Event(id=f"ev-{n}", type=type_, source="example.producer", data={"n": n}).to_envelope()
    out = sqs_record(envelope, message_id=message_id or f"m{n}", eventbridge_id=eb)
    out["eventSourceARN"] = ARN
    return out


@pytest.fixture
def harness():
    inbox, visibility, seen = MemoryInbox(), ScriptedVisibility(), []
    arns: list[str] = []

    def build_visibility(arn):
        arns.append(arn)
        return visibility

    @consumer(inbox=inbox, types=["learning.byte.completed"], metrics=CountingMetrics(), visibility=build_visibility)
    def handler(event: Event, tx) -> None:
        if event.data.get("n") == 99:
            raise RuntimeError("effect failed")
        seen.append(event.id)

    return handler, seen, visibility, arns


def test_handler_applies_each_event_once_across_a_replay(harness):
    handler, seen, _, _ = harness
    batch = {"Records": [record(1), record(1, message_id="m1b", eb="eb-replay"), record(2)]}
    assert handler(batch, None) == {"batchItemFailures": []}
    assert seen == ["ev-1", "ev-2"]


def test_handler_reports_only_the_failed_item(harness):
    handler, seen, visibility, _ = harness
    assert handler({"Records": [record(99), record(3)]}, None) == {"batchItemFailures": [{"itemIdentifier": "m99"}]}
    assert seen == ["ev-3"] and len(visibility.calls) == 1


def test_an_unwanted_type_is_acknowledged_without_calling(harness):
    handler, seen, _, _ = harness
    assert handler({"Records": [record(4, type_="other.thing")]}, None) == {"batchItemFailures": []}
    assert seen == []


def test_dependencies_are_built_once_per_queue(harness):
    handler, _, _, arns = harness
    handler({"Records": [record(5)]}, None)
    handler({"Records": [record(6)]}, None)
    assert arns == [ARN]


def test_an_empty_event_is_a_no_op(harness):
    handler, _, _, arns = harness
    assert handler({"Records": []}, None) == {"batchItemFailures": []}
    assert arns == []


def test_an_inbox_factory_is_called_lazily_once():
    calls = []

    def factory():
        calls.append(1)
        return MemoryInbox()

    @consumer(inbox=factory, metrics=CountingMetrics(), visibility=lambda arn: ScriptedVisibility())
    def handler(event, tx):
        return None

    assert calls == []
    handler({"Records": [record(1)]}, None)
    handler({"Records": [record(2)]}, None)
    assert calls == [1]


@pytest.mark.parametrize(
    ("endpoint", "url"),
    [
        (None, "https://sqs.eu-west-1.amazonaws.com/123456789012/ledger"),
        ("http://localhost:4566/", "http://localhost:4566/123456789012/ledger"),
    ],
)
def test_queue_url_from_arn(endpoint, url):
    assert queue_url_from_arn(ARN, endpoint) == url


def test_queue_url_from_arn_refuses_a_non_sqs_arn():
    with pytest.raises(ValueError):
        queue_url_from_arn("arn:aws:sns:eu-west-1:1:topic")


def test_a_filtered_type_does_not_advance_the_subject_version():
    """A widened rule delivers a foreign type at version 5. It is skipped, and the real
    event at version 5 still applies instead of reading as STALE."""
    from factbus.domain import SubjectVersion

    inbox, seen = MemoryInbox(), []

    def version_of(envelope):
        return SubjectVersion(subject_key="s-1", version=envelope["data"]["v"])

    @consumer(inbox=inbox, types=["wanted.thing"], version_of=version_of, metrics=CountingMetrics(),
              visibility=lambda arn: ScriptedVisibility())
    def handler(event, tx):
        seen.append(event.id)

    def rec(id_, type_, v):
        out = sqs_record(Event(id=id_, type=type_, source="example.producer", data={"v": v}).to_envelope(),
                         message_id=id_)
        out["eventSourceARN"] = ARN
        return out

    handler({"Records": [rec("foreign", "other.thing", 5)]}, None)
    handler({"Records": [rec("real", "wanted.thing", 5)]}, None)
    assert seen == ["real"]


@pytest.mark.parametrize(
    ("endpoint_url", "queue_url"),
    [
        (None, "https://sqs.eu-west-1.amazonaws.com/123456789012/ledger"),
        ("http://localhost:4576", "http://localhost:4576/123456789012/ledger"),
    ],
)
def test_the_default_visibility_uses_the_endpoint_argument_not_the_environment(monkeypatch, endpoint_url, queue_url):
    """factbus reads no environment: a LocalStack endpoint in the process environment
    changes nothing; only the `endpoint_url` argument does."""
    import factbus.consumer_handler as module

    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://env-should-be-ignored:1")
    monkeypatch.setenv("FACTBUS_ENDPOINT_URL", "http://env-should-be-ignored:2")
    built = []

    def fake_build(**kwargs):
        built.append(kwargs)
        return ScriptedVisibility()

    monkeypatch.setattr(module, "build_sqs_visibility", fake_build)

    @consumer(inbox=MemoryInbox(), metrics=CountingMetrics(), endpoint_url=endpoint_url)
    def handler(event, tx):
        return None

    handler({"Records": [record(1)]}, None)
    assert built == [{"queue_url": queue_url, "region_name": "eu-west-1", "endpoint_url": endpoint_url}]
