"""A Lambda handler for an SQS consumer in one decorator.

    from factbus import Event, consumer
    from factbus.adapters.dynamodb_inbox import DynamoInbox

    @consumer(inbox=DynamoInbox.from_env, types=["order.placed"])
    def handler(event: Event, tx) -> None:
        tx.put("ledger", {"pk": event.id, "total": event.data["total"]})

`handler` is now the Lambda entry point. `DynamoInbox.from_env` reads the table name from
`FACTBUS_INBOX_TABLE` (set it from the `consumer` IaC module's `inbox_table_name` output).
Per batch it builds (once per container) the inbox and the SQS visibility adapter, then runs the existing `Consumer`: dedupe on the
event id, version guard when `version_of` is given, the effect and the inbox record in
one transaction, per-message backoff and `ReportBatchItemFailures`.

`tx` is whatever the inbox hands its effect: a SQLAlchemy connection for `SqlInbox`, a
`DynamoTransaction` for `DynamoInbox`. Writes made through it commit with the inbox
record or not at all.

`types` is a second filter behind the EventBridge rule: an event of another type is
recorded in the inbox and acknowledged without calling the function, so a widened rule
cannot run code on facts it was not written for.

The queue URL is derived from the record's `eventSourceARN`, so the function needs no
queue setting. `FACTBUS_ENDPOINT_URL`, else `AWS_ENDPOINT_URL` (which LocalStack sets inside
its Lambda containers), points the SQS client at LocalStack locally.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from factbus.adapters.emf_metrics import EmfMetrics
from factbus.adapters.sqs_visibility import build_sqs_visibility
from factbus.application.consume import DEFAULT_MAX_EVENT_AGE, Consumer
from factbus.domain import BackoffSchedule, ConsumedEvent, VersionOf
from factbus.envelope import Event
from factbus.ports import Inbox, Metrics, VisibilityChanger

__all__ = ["consumer", "queue_url_from_arn"]

Handler = Callable[[Event, Any], None]


def _utc_now() -> datetime:
    return datetime.now(UTC)


def queue_url_from_arn(arn: str, endpoint_url: str | None = None) -> str:
    """`arn:aws:sqs:<region>:<account>:<name>` -> the queue URL (LocalStack-aware)."""
    parts = arn.split(":")
    if len(parts) != 6 or parts[2] != "sqs":
        raise ValueError(f"not an SQS queue ARN: {arn!r}")
    _, partition, _, region, account, name = parts
    if endpoint_url:
        return f"{endpoint_url.rstrip('/')}/{account}/{name}"
    if partition != "aws":
        raise ValueError(f"partition {partition!r} needs an explicit endpoint_url")
    return f"https://sqs.{region}.amazonaws.com/{account}/{name}"


def _region_from_arn(arn: str) -> str:
    return arn.split(":")[3]


def consumer(
    *,
    inbox: Inbox | Callable[[], Inbox],
    types: Iterable[str] | None = None,
    version_of: VersionOf | None = None,
    schedule: BackoffSchedule | None = None,
    metrics: Metrics | None = None,
    visibility: Callable[[str], VisibilityChanger] | None = None,
    service: str = "consumer",
    max_event_age: timedelta = DEFAULT_MAX_EVENT_AGE,
) -> Callable[[Handler], Callable[[Mapping[str, Any], Any], dict[str, list[dict[str, str]]]]]:
    """Wrap `fn(event, tx)` as an SQS-triggered Lambda handler.

    `inbox` is an `Inbox` or a zero-argument factory, called on the first batch so no
    client is built at import time. `visibility` builds the visibility adapter from a
    queue ARN (tests inject a double); by default it is derived from the record.
    `max_event_age` refuses older events before the inbox is read (keep it below the
    inbox's retention; see `factbus.application.consume`).
    """
    wanted = frozenset(types) if types is not None else None

    def guarded_version_of(envelope: Mapping[str, Any]) -> Any:
        # A filtered-out type must not advance its subject's version either: a widened
        # rule would otherwise make the real event at that version read as STALE.
        if wanted is not None and envelope.get("type") not in wanted:
            return None
        return version_of(envelope) if version_of is not None else None

    def decorate(fn: Handler) -> Callable[[Mapping[str, Any], Any], dict[str, list[dict[str, str]]]]:
        built: dict[str, Consumer] = {}

        def effect(tx: Any, consumed: ConsumedEvent) -> None:
            event = Event.from_envelope(consumed.envelope)
            if wanted is not None and event.type not in wanted:
                return
            fn(event, tx)

        def build(arn: str) -> Consumer:
            the_inbox = inbox() if callable(inbox) and not hasattr(inbox, "process") else inbox
            if visibility is not None:
                changer = visibility(arn)
            else:
                endpoint = os.environ.get("FACTBUS_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL") or None
                changer = build_sqs_visibility(
                    queue_url=queue_url_from_arn(arn, endpoint),
                    region_name=_region_from_arn(arn),
                    endpoint_url=endpoint,
                )
            return Consumer(
                inbox=the_inbox,
                visibility=changer,
                effect=effect,
                schedule=schedule or BackoffSchedule(),
                clock=_utc_now,
                metrics=metrics or EmfMetrics(sys.stdout, namespace="factbus", service=service),
                version_of=guarded_version_of if version_of is not None else None,
                max_event_age=max_event_age,
            )

        def handler(event: Mapping[str, Any], _context: Any) -> dict[str, list[dict[str, str]]]:
            records: Sequence[Mapping[str, Any]] = event.get("Records") or []
            if not records:
                return {"batchItemFailures": []}
            arn = str(records[0].get("eventSourceARN", ""))
            if arn not in built:
                built[arn] = build(arn)
            return built[arn].handle_batch(records)

        handler.__wrapped__ = fn  # type: ignore[attr-defined]
        return handler

    return decorate
