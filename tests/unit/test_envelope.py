"""CloudEvents 1.0 envelopes: the `Event` round trip, the three routing keys, and a
consumer that dedupes on `detail.id` while legacy `detail.event_id` keeps working."""

from __future__ import annotations

import pytest

from factbus.application import Consumer
from factbus.domain import BackoffSchedule, EnvelopeKeyMissing, envelope_keys
from factbus.envelope import Event
from tests.support import T0, CountingMetrics, ManualClock, MemoryInbox, ScriptedVisibility, envelope, sqs_record


def cloud_event(n: int) -> dict:
    return Event(
        id=f"ce-{n:05d}",
        type="learning.byte.completed",
        source="example.producer",
        subject=f"byte-{n}",
        data={"score": n},
        time=T0.isoformat(),  # the consumer's ManualClock starts at T0
    ).to_envelope()


def test_event_round_trips_through_its_envelope():
    event = Event(type="t.happened", source="svc", subject="s-1", data={"a": 1}, extensions={"orgid": "o-1"})
    envelope_dict = event.to_envelope()
    assert envelope_dict["specversion"] == "1.0"
    assert envelope_dict["orgid"] == "o-1"
    assert Event.from_envelope(envelope_dict) == event


def test_event_reads_a_legacy_envelope():
    legacy = envelope(7)
    event = Event.from_envelope(legacy)
    assert (event.id, event.type, event.source) == envelope_keys(legacy)


def test_event_reads_a_legacy_envelope_that_carries_its_contract_in_specversion():
    legacy = envelope(7, specversion="comms.event/v1")
    event = Event.from_envelope(legacy)
    assert (event.id, event.type, event.source) == envelope_keys(legacy)
    assert "specversion" not in event.extensions
    assert event.to_envelope()["specversion"] == "1.0"


def test_event_ids_default_to_unique_uuids():
    assert Event(type="t", source="s").id != Event(type="t", source="s").id


@pytest.mark.parametrize("name", ["type", "source", "id"])
def test_event_refuses_an_empty_core_attribute(name):
    values = {"type": "t", "source": "s", "id": "i", name: ""}
    with pytest.raises(ValueError):
        Event(**values)


def test_event_refuses_an_extension_that_shadows_a_core_attribute():
    with pytest.raises(ValueError):
        Event(type="t", source="s", extensions={"id": "x"})


def test_envelope_keys_read_cloudevents_names():
    assert envelope_keys(cloud_event(1)) == ("ce-00001", "learning.byte.completed", "example.producer")


@pytest.mark.parametrize("key", ["id", "type", "source"])
def test_envelope_keys_refuse_a_cloudevent_missing_a_key(key):
    body = cloud_event(1)
    body[key] = ""
    with pytest.raises(EnvelopeKeyMissing):
        envelope_keys(body)


def test_a_cloudevent_without_specversion_1_0_is_read_with_legacy_names():
    body = cloud_event(1)
    body["specversion"] = "0.3"
    with pytest.raises(EnvelopeKeyMissing):
        envelope_keys(body)  # no `event_id` / `producer`


class Recorder:
    def __init__(self) -> None:
        self.applied: list[str] = []

    def __call__(self, connection, event) -> None:
        self.applied.append(event.event_id)


def test_consumer_dedupes_cloudevents_on_detail_id_and_still_reads_legacy():
    effects = Recorder()
    consumer = Consumer(
        inbox=MemoryInbox(), visibility=ScriptedVisibility(), effect=effects, schedule=BackoffSchedule(),
        clock=ManualClock(), metrics=CountingMetrics(),
    )
    records = [
        sqs_record(cloud_event(1), message_id="m1", eventbridge_id="eb-1"),
        sqs_record(cloud_event(1), message_id="m2", eventbridge_id="eb-replay"),
        sqs_record(envelope(2), message_id="m3"),
    ]
    assert consumer.handle_batch(records) == {"batchItemFailures": []}
    assert effects.applied == ["ce-00001", "evt-00002"]


def test_consumer_reports_a_cloudevent_without_an_id():
    body = cloud_event(1)
    del body["id"]
    visibility = ScriptedVisibility()
    consumer = Consumer(
        inbox=MemoryInbox(), visibility=visibility, effect=Recorder(), schedule=BackoffSchedule(),
        clock=ManualClock(), metrics=CountingMetrics(),
    )
    assert consumer.handle_batch([sqs_record(body, message_id="m1")]) == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
