"""Turn one SQS batch into effects plus a failure report (design §2, K1, K3).

The Lambda event source mapping is configured with `ReportBatchItemFailures`, so the
handler returns `{"batchItemFailures": [{"itemIdentifier": <messageId>}, ...]}` and only
the listed messages stay on the queue; the rest are deleted. One bad message does not
replay its nine neighbours.

Per record, in order:

1. Read the transport fields: `messageId`, `receiptHandle`, `ApproximateReceiveCount`.
   A record without them cannot be reported item by item, so the whole batch fails.
2. Parse the EventBridge body and take `detail.event_id`. A malformed record is
   reported with backoff; after the queue's `maxReceiveCount` it reaches the DLQ (F9).
   An envelope without its own readable `time` or `occurred_at` is malformed too: its
   age is unknown, and EventBridge `time` alone is reset when the same id is re-put.
   So is an envelope dated more than `MAX_CLOCK_SKEW` in the future: it would make
   every later replay look young.
3. Refuse an event older than `max_event_age` (default 30 days) BEFORE the inbox is
   read. Inbox and claim rows are purged after 35 days, so an older replay could meet a
   key whose row is gone and apply again; refused, it is reported with backoff and
   reaches the DLQ, where an operator sees it. The age is measured from the EARLIEST of
   the EventBridge `time` and the envelope's own `time` (CloudEvents) or `occurred_at`
   (legacy): every one of them precedes the first time any consumer could have
   recorded the event, so the check never under-states how old the inbox row could be.
4. While the provider breaker is open and no probe is due, report with backoff
   without calling anything.
5. `Inbox.process`: dedupe, version guard, effect and inbox row in ONE transaction.
   DUPLICATE and STALE are successes (C0b-17, K3) and need no provider slot.
6. Immediately before the effect, take an available rate token (design §2).
   BLOCK and retry against a fresh clock until one is available. Blocking, not
   deferring, is the normal path: a deferral spends an `ApproximateReceiveCount`,
   so deferring a 10,000-item backlog would dead-letter it after 10 receives.
   If the wait exceeds `rate_max_wait`, roll back the inbox transaction and report
   backoff; the item is never dropped and no breaker probe has started.
7. On any failure, set the message's visibility from the schedule and report it. If
   that call itself fails, the item is STILL reported (C0b-19): it then returns after
   the queue's default visibility, which only spends a receive sooner.

Nothing here raises for a single record's failure; a raised exception would fail every
record in the batch, including those whose effects already committed.

One `Consumer` per instance, used by one thread: the breaker and the rate guard hold
in-process state and take no lock. A Lambda instance runs one invocation at a time, so
this is the production shape; a harness running several pollers in one process gives
each its own `Consumer`.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from factbus.domain import (
    MAX_EVENT_AGE,
    BackoffSchedule,
    ConsumedEvent,
    InboxOutcome,
    ProviderBreaker,
    TokenBucket,
    VersionOf,
)
from factbus.ports import Clock, Effect, Inbox, Metrics, ProviderError, Sleep, VisibilityChanger

__all__ = [
    "DEFAULT_MAX_EVENT_AGE",
    "DEFAULT_RATE_MAX_WAIT",
    "BatchReport",
    "Consumer",
    "MalformedRecord",
    "TransportFieldsMissing",
]

log = logging.getLogger(__name__)

APPLIED_METRIC = "ConsumerApplied"
DUPLICATE_METRIC = "ConsumerDuplicates"
STALE_METRIC = "ConsumerStale"
MALFORMED_METRIC = "ConsumerMalformed"
RETRY_METRIC = "ConsumerRetried"
PROVIDER_ERROR_METRIC = "ConsumerProviderErrors"
BREAKER_DEFERRED_METRIC = "ConsumerBreakerDeferred"
VISIBILITY_FAILED_METRIC = "ConsumerVisibilityCallFailed"
RATE_DEFERRED_METRIC = "ConsumerRateDeferred"
EXPIRED_METRIC = "ConsumerExpired"

#: The oldest event the consumer applies; see step 3 above and `factbus.domain.MAX_EVENT_AGE`.
DEFAULT_MAX_EVENT_AGE = MAX_EVENT_AGE

#: How far in the future an envelope's own timestamp may be before the record is
#: malformed. Small next to the 5 days between `MAX_EVENT_AGE` and `ROW_RETENTION`.
MAX_CLOCK_SKEW = timedelta(minutes=5)

#: The longest one item blocks for its provider slot. A batch runs its items in turn, so
#: `batch_size / instance rate` plus the effects' own time must fit inside the function
#: timeout; the default covers a batch of 10 at 2 calls per second.
DEFAULT_RATE_MAX_WAIT = timedelta(seconds=5)


class TransportFieldsMissing(ValueError):
    """A record lacks `messageId` or `receiptHandle`: it cannot be reported on its own."""


class _RateDeferred(Exception):
    """Leave the inbox transaction without committing a rate-blocked item."""


class MalformedRecord(ValueError):
    """The body is not an EventBridge event carrying a non-empty `detail.id` (CloudEvents)
    or `detail.event_id` (legacy envelope)."""


@dataclass(frozen=True)
class _Delivery:
    message_id: str
    receipt_handle: str
    receive_count: int


@dataclass
class BatchReport:
    """What one batch did, per outcome. The Lambda response is `response`."""

    applied: list[str] = field(default_factory=list)
    duplicate: list[str] = field(default_factory=list)
    stale: list[str] = field(default_factory=list)
    #: Refused as older than `max_event_age`; each is also in `failed`.
    expired: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    visibility_failed: list[str] = field(default_factory=list)
    waits: dict[str, int] = field(default_factory=dict)

    @property
    def response(self) -> dict[str, list[dict[str, str]]]:
        return {"batchItemFailures": [{"itemIdentifier": message_id} for message_id in self.failed]}


class Consumer:
    def __init__(
        self,
        *,
        inbox: Inbox,
        visibility: VisibilityChanger,
        effect: Effect,
        schedule: BackoffSchedule,
        clock: Clock,
        metrics: Metrics,
        version_of: VersionOf | None = None,
        breaker: ProviderBreaker | None = None,
        rate_guard: TokenBucket | None = None,
        rate_max_wait: timedelta = DEFAULT_RATE_MAX_WAIT,
        sleep: Sleep | None = None,
        max_event_age: timedelta = DEFAULT_MAX_EVENT_AGE,
    ) -> None:
        if rate_guard is not None and sleep is None:
            raise ValueError("a rate guard blocks, so it needs `sleep` (time.sleep in production)")
        if max_event_age <= timedelta(0):
            raise ValueError("max_event_age must be positive")
        self._inbox = inbox
        self._visibility = visibility
        self._effect = effect
        self._schedule = schedule
        self._clock = clock
        self._metrics = metrics
        self._version_of = version_of
        self._breaker = breaker
        self._rate_guard = rate_guard
        self._rate_max_wait = rate_max_wait
        self._sleep = sleep
        self._max_event_age = max_event_age

    def handle_batch(self, records: Sequence[Mapping[str, object]]) -> dict[str, list[dict[str, str]]]:
        """The Lambda handler's body: returns the `ReportBatchItemFailures` response."""
        return self.process_batch(records).response

    def process_batch(self, records: Sequence[Mapping[str, object]]) -> BatchReport:
        deliveries = [_delivery(record) for record in records]
        report = BatchReport()
        for record, delivery in zip(records, deliveries, strict=True):
            self._consume_one(record, delivery, report)
        return report

    def _consume_one(self, record: Mapping[str, object], delivery: _Delivery, report: BatchReport) -> None:
        try:
            event, published_at = _consumed_event(record, delivery, self._clock())
            subject = self._version_of(event.envelope) if self._version_of else None
        except (MalformedRecord, KeyError, TypeError, ValueError) as error:
            self._metrics.count(MALFORMED_METRIC)
            log.warning("malformed record %s: %s", delivery.message_id, error)
            self._defer(delivery, report)
            return

        age = self._clock() - published_at
        if age > self._max_event_age:
            self._metrics.count(EXPIRED_METRIC)
            log.warning(
                "refused %s: %s old, past the %s age limit; its inbox row may be purged",
                event.event_id, age, self._max_event_age,
            )
            report.expired.append(delivery.message_id)
            self._defer(delivery, report)
            return

        if self._breaker is not None and not self._breaker.admits(self._clock()):
            self._metrics.count(BREAKER_DEFERRED_METRIC)
            self._defer(delivery, report)
            return
        try:
            outcome = self._inbox.process(event, subject=subject, effect=self._guarded_effect)
        except _RateDeferred:
            self._metrics.count(RATE_DEFERRED_METRIC)
            self._defer(delivery, report)
            return
        except ProviderError as error:
            self._metrics.count(PROVIDER_ERROR_METRIC)
            if self._breaker is not None:
                self._breaker.record_failure(self._clock())
            log.warning("provider error on %s: %s", event.event_id, error)
            self._defer(delivery, report)
            return
        except Exception as error:  # every failure is retried with backoff, then dead-lettered
            log.warning("effect failed on %s: %s: %s", event.event_id, type(error).__name__, error)
            self._defer(delivery, report)
            return

        if outcome is InboxOutcome.APPLIED:
            if self._breaker is not None:
                self._breaker.record_success()
            self._metrics.count(APPLIED_METRIC)
            report.applied.append(delivery.message_id)
        elif outcome is InboxOutcome.DUPLICATE:
            self._metrics.count(DUPLICATE_METRIC)
            report.duplicate.append(delivery.message_id)
        else:
            self._metrics.count(STALE_METRIC)
            report.stale.append(delivery.message_id)

    def _guarded_effect(self, connection, event: ConsumedEvent) -> None:
        # Inbox lock/transaction work precedes this callback, so it cannot turn an
        # earlier reservation into a delayed burst at the actual effect boundary.
        if not self._await_provider_slot():
            raise _RateDeferred()
        if self._breaker is not None:
            self._breaker.probe_started(self._clock())
        self._effect(connection, event)

    def _await_provider_slot(self) -> bool:
        """Take a token only when available, rechecking after every sleep.

        The deadline bounds actual elapsed waiting, including an oversleep. Read
        the initial clock once so max_wait=0 still admits an available token.
        """
        if self._rate_guard is None:
            return True
        now = self._clock()
        deadline = now + self._rate_max_wait
        while now <= deadline:
            wait = self._rate_guard.try_take(now)
            if wait == timedelta(0):
                return True
            if wait > deadline - now:
                return False
            self._sleep(wait.total_seconds())
            now = self._clock()
        return False

    def _defer(self, delivery: _Delivery, report: BatchReport) -> None:
        """Hide the message for its backoff wait and report it. Reporting never depends
        on the visibility call succeeding (C0b-19)."""
        seconds = self._schedule.wait_for(delivery.receive_count)
        report.failed.append(delivery.message_id)
        report.waits[delivery.message_id] = seconds
        self._metrics.count(RETRY_METRIC)
        try:
            self._visibility.change_visibility(delivery.receipt_handle, seconds)
        except Exception as error:  # C0b-19: any failure here still leaves the item reported
            self._metrics.count(VISIBILITY_FAILED_METRIC)
            report.visibility_failed.append(delivery.message_id)
            log.warning("ChangeMessageVisibility failed for %s: %s", delivery.message_id, error)


def _delivery(record: Mapping[str, object]) -> _Delivery:
    message_id = record.get("messageId")
    receipt_handle = record.get("receiptHandle")
    if not isinstance(message_id, str) or not message_id or not isinstance(receipt_handle, str) or not receipt_handle:
        raise TransportFieldsMissing("an SQS record needs messageId and receiptHandle")
    attributes = record.get("attributes")
    raw_count = attributes.get("ApproximateReceiveCount") if isinstance(attributes, Mapping) else None
    try:
        receive_count = int(raw_count) if raw_count is not None else 1
    except (TypeError, ValueError):
        receive_count = 1
    return _Delivery(message_id, receipt_handle, receive_count)


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        moment = datetime.fromisoformat(value)
    except ValueError:
        return None
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def _published_at(body: Mapping[str, object], detail: Mapping[str, object], now: datetime) -> datetime:
    """The earliest readable timestamp the event carries (see step 3 above)."""
    # The envelope's own stamp is required: EventBridge `time` alone is not stable, since a
    # re-put of the same event id gets a fresh one and would pass the age check after its
    # inbox row was purged. EventBridge `time` still counts when it is the earliest.
    own = [moment for moment in (_timestamp(detail.get("time")), _timestamp(detail.get("occurred_at"))) if moment]
    if not own:
        raise MalformedRecord("the envelope carries no readable time or occurred_at")
    if min(own) > now + MAX_CLOCK_SKEW:
        # A future stamp would make every later replay look young, so a re-put after
        # the inbox row's purge would pass the age check and apply again.
        raise MalformedRecord(f"the envelope is dated {min(own) - now} in the future")
    transport = _timestamp(body.get("time"))
    return min([*own, transport] if transport is not None else own)


def _consumed_event(
    record: Mapping[str, object], delivery: _Delivery, now: datetime
) -> tuple[ConsumedEvent, datetime]:
    body = record.get("body")
    if not isinstance(body, str):
        raise MalformedRecord("record body is not a string")
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as error:
        raise MalformedRecord(f"record body is not JSON: {error}") from error
    detail = parsed.get("detail") if isinstance(parsed, dict) else None
    if not isinstance(detail, dict):
        raise MalformedRecord("record body carries no EventBridge detail object")
    key = "id" if detail.get("specversion") == "1.0" else "event_id"
    event_id = detail.get(key)
    if not isinstance(event_id, str) or not event_id:
        raise MalformedRecord(f"detail.{key} is missing or empty")
    event = ConsumedEvent(
        event_id=event_id,
        envelope=detail,
        message_id=delivery.message_id,
        receive_count=delivery.receive_count,
    )
    return event, _published_at(parsed, detail, now)
