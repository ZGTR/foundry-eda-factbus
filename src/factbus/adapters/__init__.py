"""I/O for the outbox and the consumer: EventBridge, SQS visibility, the SQL tables and
their migrations, the DynamoDB inbox, the claim stores behind `once`, EMF metrics.

Nothing here constructs itself. A composition root calls `build_eventbridge_bus` and
the constructors below and injects the results into the application layer.

The SQL names load on first use (PEP 562), so a Lambda consumer that only needs
`DynamoInbox` never imports SQLAlchemy or Alembic. `from factbus.adapters import SqlInbox`
(or `SqlClaimStore`) works as before.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

from factbus.adapters.dynamodb_inbox import (
    DYNAMODB_CLIENT_CONFIG,
    DynamoInbox,
    DynamoTransaction,
    TransactionTooLarge,
    build_dynamo_inbox,
)
from factbus.adapters.emf_metrics import EmfMetrics
from factbus.adapters.eventbridge import EVENTBRIDGE_CLIENT_CONFIG, EventBridgeBus, build_eventbridge_bus
from factbus.adapters.sqs_visibility import SQS_CLIENT_CONFIG, SqsVisibility, build_sqs_visibility

if TYPE_CHECKING:
    from factbus.adapters.sql import DEFAULT_TABLE_NAME, SqlOutboxStore, due_rows_query, outbox_table
    from factbus.adapters.sql_claims import DEFAULT_CLAIMS_TABLE_NAME, SqlClaimStore, claims_table
    from factbus.adapters.sql_inbox import (
        DEFAULT_INBOX_TABLE_NAME,
        DEFAULT_VERSIONS_TABLE_NAME,
        SqlInbox,
        applied_versions_table,
        inbox_table,
    )
    from factbus.adapters.sql_migration import (
        downgrade_0001,
        downgrade_claims_0001,
        downgrade_inbox_0001,
        downgrade_inbox_0002,
        upgrade_0001,
        upgrade_claims_0001,
        upgrade_inbox_0001,
        upgrade_inbox_0002,
    )

#: Each SQLAlchemy-backed name, mapped to the submodule (relative to this package) that defines it.
_LAZY = {
    "DEFAULT_TABLE_NAME": ".sql",
    "SqlOutboxStore": ".sql",
    "due_rows_query": ".sql",
    "outbox_table": ".sql",
    "DEFAULT_CLAIMS_TABLE_NAME": ".sql_claims",
    "SqlClaimStore": ".sql_claims",
    "claims_table": ".sql_claims",
    "DEFAULT_INBOX_TABLE_NAME": ".sql_inbox",
    "DEFAULT_VERSIONS_TABLE_NAME": ".sql_inbox",
    "SqlInbox": ".sql_inbox",
    "applied_versions_table": ".sql_inbox",
    "inbox_table": ".sql_inbox",
    "downgrade_0001": ".sql_migration",
    "downgrade_claims_0001": ".sql_migration",
    "downgrade_inbox_0001": ".sql_migration",
    "downgrade_inbox_0002": ".sql_migration",
    "upgrade_0001": ".sql_migration",
    "upgrade_claims_0001": ".sql_migration",
    "upgrade_inbox_0001": ".sql_migration",
    "upgrade_inbox_0002": ".sql_migration",
}

__all__ = [
    "DEFAULT_CLAIMS_TABLE_NAME",
    "DEFAULT_INBOX_TABLE_NAME",
    "DEFAULT_TABLE_NAME",
    "DEFAULT_VERSIONS_TABLE_NAME",
    "DYNAMODB_CLIENT_CONFIG",
    "SQS_CLIENT_CONFIG",
    "EVENTBRIDGE_CLIENT_CONFIG",
    "DynamoInbox",
    "DynamoTransaction",
    "EmfMetrics",
    "EventBridgeBus",
    "SqlClaimStore",
    "SqlInbox",
    "SqlOutboxStore",
    "SqsVisibility",
    "TransactionTooLarge",
    "applied_versions_table",
    "build_dynamo_inbox",
    "build_eventbridge_bus",
    "build_sqs_visibility",
    "claims_table",
    "downgrade_0001",
    "downgrade_claims_0001",
    "downgrade_inbox_0001",
    "downgrade_inbox_0002",
    "due_rows_query",
    "inbox_table",
    "outbox_table",
    "upgrade_0001",
    "upgrade_claims_0001",
    "upgrade_inbox_0001",
    "upgrade_inbox_0002",
]


def __getattr__(name: str) -> Any:
    module = _LAZY.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(module, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_LAZY))
