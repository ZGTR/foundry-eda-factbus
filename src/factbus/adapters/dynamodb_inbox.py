"""The consumer inbox on DynamoDB: dedupe, version guard and the service's own writes in
ONE `TransactWriteItems`, for a Lambda consumer with no SQL database and no VPC.

`DynamoInbox.process` keeps the semantics of `adapters.sql_inbox.SqlInbox.process`:

1. consistent reads of the inbox item and, when the event has an ordered subject, the
   subject's version item. The inbox item present means DUPLICATE. An applied version
   at or above the event's means STALE: the inbox item alone is recorded, with no
   effect, so a later copy is a plain duplicate. Neither path calls the effect, just as
   the SQL inbox calls it only after both checks pass;
2. a `DynamoTransaction` holding the conditional inbox Put (`attribute_not_exists(pk)`)
   and, with a subject, the conditional version Update
   (`attribute_not_exists(pk) OR version < :v`);
3. `effect(tx, event)`: the service adds its own Put, Update and Delete items to `tx`.
   It sends nothing itself, so an effect that raises leaves nothing written;
4. one `transact_write_items`. Everything commits, or nothing does.

The reads in step 1 only save the effect a wasted call; the conditions in step 2
decide. A copy that another worker records between the read and the commit cancels the
transaction on the inbox item's `ConditionalCheckFailed`: DUPLICATE (C0b-17). A newer
version written in that window cancels it on the version item: the write is retried
once with the inbox Put alone and the outcome is STALE (K3). Any other cancellation,
including a condition the effect set on its own item, propagates as the `ClientError`,
and nothing was written.

Key shape (single table, string `pk` HASH and string `sk` RANGE, TTL on `expires_at`):

    inbox item     pk = "inbox#<event_id>"       sk = "inbox"    received_at, expires_at
    version item   pk = "version#<subject_key>"  sk = "version"  version, updated_at

The effect may write to the same table under its own `pk` prefixes, or to other tables
in the same account and region. Version items carry no TTL: forgetting a subject's
applied version would let an older copy apply over a newer one.

RETENTION. The inbox item is the only record that `event_id` was handled; once TTL
deletes it, a copy is applied again. A copy can arrive as late as the source queue's
`MessageRetentionPeriod` (up to 14 days) plus the DLQ's: a message can sit out the
first on the source queue, then most of the second on the DLQ before a redrive puts it
back on the source queue. `retention` must exceed that sum, and the age of the oldest
EventBridge archive window you would replay. The default, 35 days, covers a queue and DLQ
at the 14-day maximum each and the `bus` module's archive, which is capped below 35 days. TTL deletion lags expiry by up to a few days; an
expired item that still exists still dedupes, so the lag only errs toward DUPLICATE.

Limits: `TransactWriteItems` takes at most 100 items and 4 MB in total, and touches
each item at most once. The inbox uses one item, two with a subject;
`DynamoTransaction` refuses the 101st before anything is sent.

Construction happens in `build_dynamo_inbox`, called by a composition root.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import boto3
import botocore.exceptions
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.config import Config

from factbus.domain import ConsumedEvent, InboxOutcome, SubjectVersion, is_newer
from factbus.ports import Clock, Effect

__all__ = [
    "DEFAULT_INBOX_RETENTION",
    "DYNAMODB_CLIENT_CONFIG",
    "MAX_TRANSACT_ITEMS",
    "DynamoInbox",
    "DynamoTransaction",
    "TransactionTooLarge",
    "build_dynamo_inbox",
]

#: The `TransactWriteItems` ceiling, counting the inbox's own one or two items.
MAX_TRANSACT_ITEMS = 100

DEFAULT_INBOX_RETENTION = timedelta(days=35)

INBOX_SORT_KEY = "inbox"
VERSION_SORT_KEY = "version"

#: Three attempts. An SDK re-send of a transaction that committed but whose response
#: was lost cancels on the inbox condition and reads as DUPLICATE: the effect still
#: committed exactly once, so the re-send cannot duplicate anything.
DYNAMODB_CLIENT_CONFIG = Config(
    retries={"total_max_attempts": 3, "mode": "standard"},
    connect_timeout=2,
    read_timeout=5,
    # factbus reads no environment, and neither may its clients: without this, boto3
    # takes AWS_ENDPOINT_URL (or a config-file endpoint) when `endpoint_url` is None.
    ignore_configured_endpoint_urls=True,
    # The same holds for the endpoint variants: AWS_USE_FIPS_ENDPOINT and
    # AWS_USE_DUALSTACK_ENDPOINT would otherwise pick a different host. A caller that
    # needs one passes it as `endpoint_url`.
    use_fips_endpoint=False,
    use_dualstack_endpoint=False,
)

_CONDITION_FAILED = "ConditionalCheckFailed"
_NOT_RECORDED = "attribute_not_exists(pk)"


class TransactionTooLarge(ValueError):
    """An effect tried to add a write past the `TransactWriteItems` item limit."""


class DynamoTransaction:
    """The writes of one `TransactWriteItems`, collected and not yet sent.

    The effect receives this as its `connection`. Items, keys and expression values are
    plain Python (`str`, `int`, `Decimal`, `bytes`, `bool`, `None`, lists, mappings and
    sets), serialised with boto3's `TypeSerializer`; a `float` is refused there, as in
    boto3's resource API. A condition that fails cancels the whole transaction.
    """

    def __init__(self, *, limit: int = MAX_TRANSACT_ITEMS) -> None:
        if not 1 <= limit <= MAX_TRANSACT_ITEMS:
            raise ValueError(f"limit must be between 1 and {MAX_TRANSACT_ITEMS}, got {limit!r}")
        self._limit = limit
        self._items: list[dict[str, Any]] = []

    def __len__(self) -> int:
        return len(self._items)

    @property
    def items(self) -> list[dict[str, Any]]:
        """The `TransactItems` request list, in the order the writes were added."""
        return list(self._items)

    def put(
        self,
        table: str,
        item: Mapping[str, Any],
        condition: str | None = None,
        *,
        names: Mapping[str, str] | None = None,
        values: Mapping[str, Any] | None = None,
    ) -> None:
        self._add("Put", table, {"Item": _serialise(item)}, condition, names, values)

    def update(
        self,
        table: str,
        key: Mapping[str, Any],
        expression: str,
        condition: str | None = None,
        *,
        names: Mapping[str, str] | None = None,
        values: Mapping[str, Any] | None = None,
    ) -> None:
        if not expression:
            raise ValueError("an update needs an UpdateExpression")
        self._add("Update", table, {"Key": _serialise(key), "UpdateExpression": expression}, condition, names, values)

    def delete(
        self,
        table: str,
        key: Mapping[str, Any],
        condition: str | None = None,
        *,
        names: Mapping[str, str] | None = None,
        values: Mapping[str, Any] | None = None,
    ) -> None:
        self._add("Delete", table, {"Key": _serialise(key)}, condition, names, values)

    def _add(
        self,
        action: str,
        table: str,
        request: dict[str, Any],
        condition: str | None,
        names: Mapping[str, str] | None,
        values: Mapping[str, Any] | None,
    ) -> None:
        if len(self._items) >= self._limit:
            raise TransactionTooLarge(
                f"a DynamoDB transaction holds at most {self._limit} writes, the inbox's own included; "
                f"refusing write {len(self._items) + 1} ({action} on {table!r})"
            )
        if not table:
            raise ValueError("table is required")
        request = {"TableName": table, **request}
        if condition is not None:
            request["ConditionExpression"] = condition
        if names:
            request["ExpressionAttributeNames"] = dict(names)
        if values:
            request["ExpressionAttributeValues"] = _serialise(values)
        self._items.append({action: request})


class DynamoInbox:
    def __init__(
        self,
        client,
        table_name: str,
        *,
        retention: timedelta = DEFAULT_INBOX_RETENTION,
        clock: Clock,
    ) -> None:
        if not table_name:
            raise ValueError("table_name is required")
        if retention <= timedelta(0):
            raise ValueError(f"retention must be positive, got {retention!r}")
        self._client = client
        self._table = table_name
        self._retention = retention
        self._clock = clock

    @property
    def table_name(self) -> str:
        return self._table

    def process(self, event: ConsumedEvent, *, subject: SubjectVersion | None, effect: Effect) -> InboxOutcome:
        if self.contains(event.event_id):
            return InboxOutcome.DUPLICATE
        if subject is not None and not is_newer(self.applied_version(subject.subject_key), subject.version):
            return self._record_stale(event)

        now = self._clock()
        tx = DynamoTransaction()
        tx.put(self._table, self._inbox_item(event.event_id, now), _NOT_RECORDED)
        if subject is not None:
            tx.update(
                self._table,
                _version_key(subject.subject_key),
                "SET #version = :version, updated_at = :now",
                "attribute_not_exists(pk) OR #version < :version",
                names={"#version": "version"},
                values={":version": subject.version, ":now": now.isoformat()},
            )
        effect(tx, event)
        try:
            self._client.transact_write_items(TransactItems=tx.items)
        except botocore.exceptions.ClientError as error:
            codes = _cancellation_codes(error)
            if codes and codes[0] == _CONDITION_FAILED:
                return InboxOutcome.DUPLICATE
            if subject is not None and len(codes) > 1 and codes[1] == _CONDITION_FAILED:
                return self._record_stale(event)
            raise
        return InboxOutcome.APPLIED

    def contains(self, event_id: str) -> bool:
        response = self._client.get_item(
            TableName=self._table, Key=_serialise(_inbox_key(event_id)), ConsistentRead=True, ProjectionExpression="pk"
        )
        return "Item" in response

    def applied_version(self, subject_key: str) -> int | None:
        response = self._client.get_item(
            TableName=self._table,
            Key=_serialise(_version_key(subject_key)),
            ConsistentRead=True,
            ProjectionExpression="#version",
            ExpressionAttributeNames={"#version": "version"},
        )
        item = response.get("Item")
        if not item or "version" not in item:
            return None
        return int(_DESERIALISER.deserialize(item["version"]))

    def _record_stale(self, event: ConsumedEvent) -> InboxOutcome:
        """Record the inbox item with no effect, so a later copy is a plain duplicate."""
        try:
            self._client.put_item(
                TableName=self._table,
                Item=_serialise(self._inbox_item(event.event_id, self._clock())),
                ConditionExpression=_NOT_RECORDED,
            )
        except botocore.exceptions.ClientError as error:
            if error.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return InboxOutcome.DUPLICATE
            raise
        return InboxOutcome.STALE

    def _inbox_item(self, event_id: str, now: datetime) -> dict[str, Any]:
        return {
            **_inbox_key(event_id),
            "received_at": now.isoformat(),
            "expires_at": int((now + self._retention).timestamp()),
        }


def _utc_now() -> datetime:
    return datetime.now(UTC)


def build_dynamo_inbox(
    *,
    table_name: str,
    region_name: str,
    clock: Clock | None = None,
    retention: timedelta = DEFAULT_INBOX_RETENTION,
    endpoint_url: str | None = None,
    session: boto3.session.Session | None = None,
) -> DynamoInbox:
    """Build the adapter. Every setting is an argument: factbus reads no environment, so
    the caller's own configuration names the table, the region and, locally (LocalStack),
    `endpoint_url`, which stays None in AWS. `clock` defaults to UTC now."""
    if not table_name:
        raise ValueError("table_name is required")
    if not region_name:
        raise ValueError("region_name is required")
    client = (session or boto3.session.Session()).client(
        "dynamodb",
        region_name=region_name,
        endpoint_url=endpoint_url,
        config=DYNAMODB_CLIENT_CONFIG,
    )
    return DynamoInbox(client, table_name, retention=retention, clock=clock or _utc_now)


_SERIALISER = TypeSerializer()
_DESERIALISER = TypeDeserializer()


def _serialise(attributes: Mapping[str, Any]) -> dict[str, Any]:
    return {name: _SERIALISER.serialize(_decimals(value)) for name, value in attributes.items()}


def _decimals(value: Any) -> Any:
    """JSON numbers arrive as floats, which boto3 refuses for DynamoDB. Convert each
    through its shortest repr, so 19.99 is stored as Decimal("19.99"), not as the float's
    binary expansion."""
    if isinstance(value, float):
        return Decimal(repr(value))
    if isinstance(value, Mapping):
        return {key: _decimals(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_decimals(item) for item in value]
    return value


def _inbox_key(event_id: str) -> dict[str, str]:
    return {"pk": f"inbox#{event_id}", "sk": INBOX_SORT_KEY}


def _version_key(subject_key: str) -> dict[str, str]:
    return {"pk": f"version#{subject_key}", "sk": VERSION_SORT_KEY}


def _cancellation_codes(error: botocore.exceptions.ClientError) -> list[str]:
    """Per-item codes of a cancelled transaction, in request order; [] for any other error."""
    if error.response.get("Error", {}).get("Code") != "TransactionCanceledException":
        return []
    return [str(reason.get("Code", "")) for reason in error.response.get("CancellationReasons") or []]
