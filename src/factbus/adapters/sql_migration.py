"""Migration 0001 creates the outbox table; inbox 0001 creates a consumer's inbox and
applied-version tables; claims 0001 creates the table behind `SqlClaimStore`. All
frozen; a later change is a new migration.

A service's own Alembic chain owns its revision ids, so this module ships the DDL as
functions a revision calls:

    from factbus.adapters.sql_migration import upgrade_0001, downgrade_0001

    def upgrade():
        upgrade_0001(op)

The columns are written out here rather than derived from `outbox_table`, because a
migration that follows the live table definition rewrites history the day the table
changes. `tests/integration/test_sql_outbox.py` reflects the migrated table on both
engines and compares it with `outbox_table`, so the two cannot drift silently; the
inbox and claims suites do the same for theirs.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import mysql

from factbus.adapters.sql import DEFAULT_TABLE_NAME
from factbus.adapters.sql_claims import CLAIM_KEY_TYPE, DEFAULT_CLAIMS_TABLE_NAME
from factbus.adapters.sql_inbox import DEFAULT_INBOX_TABLE_NAME, DEFAULT_VERSIONS_TABLE_NAME

__all__ = [
    "downgrade_0001",
    "downgrade_claims_0001",
    "downgrade_inbox_0001",
    "downgrade_inbox_0002",
    "upgrade_0001",
    "upgrade_claims_0001",
    "upgrade_inbox_0001",
    "upgrade_inbox_0002",
]

_TIMESTAMP = sa.DateTime().with_variant(mysql.DATETIME(fsp=6), "mysql")


def upgrade_0001(op, table_name: str = DEFAULT_TABLE_NAME) -> None:
    op.create_table(
        table_name,
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True, autoincrement=True),
        sa.Column("event_id", sa.String(128), nullable=False),
        sa.Column("event_type", sa.String(255), nullable=False),
        sa.Column("producer", sa.String(255), nullable=False),
        sa.Column("envelope", sa.Text().with_variant(mysql.MEDIUMTEXT(), "mysql"), nullable=False),
        sa.Column("created_at", _TIMESTAMP, nullable=False),
        sa.Column("sent_at", _TIMESTAMP, nullable=True),
        sa.Column("failed_at", _TIMESTAMP, nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.String(255), nullable=True),
        sa.UniqueConstraint("event_id", name=f"uq_{table_name}_event_id"),
    )
    op.create_index(f"ix_{table_name}_due", table_name, ["sent_at", "failed_at", "id"])


def downgrade_0001(op, table_name: str = DEFAULT_TABLE_NAME) -> None:
    op.drop_index(f"ix_{table_name}_due", table_name=table_name)
    op.drop_table(table_name)


def upgrade_inbox_0001(
    op,
    inbox_name: str = DEFAULT_INBOX_TABLE_NAME,
    versions_name: str = DEFAULT_VERSIONS_TABLE_NAME,
) -> None:
    """A consumer's two tables. A producer that also consumes runs both 0001s."""
    op.create_table(
        inbox_name,
        sa.Column("event_id", sa.String(128), primary_key=True),
        sa.Column("received_at", _TIMESTAMP, nullable=False),
    )
    op.create_table(
        versions_name,
        sa.Column("subject_key", sa.String(255), primary_key=True),
        sa.Column("version", sa.BigInteger(), nullable=False),
        sa.Column("updated_at", _TIMESTAMP, nullable=False),
    )


def downgrade_inbox_0001(
    op,
    inbox_name: str = DEFAULT_INBOX_TABLE_NAME,
    versions_name: str = DEFAULT_VERSIONS_TABLE_NAME,
) -> None:
    op.drop_table(versions_name)
    op.drop_table(inbox_name)


def upgrade_inbox_0002(op, inbox_name: str = DEFAULT_INBOX_TABLE_NAME) -> None:
    """Index `received_at` so `SqlInbox.purge` reads only the rows it deletes."""
    op.create_index(f"ix_{inbox_name}_received_at", inbox_name, ["received_at"])


def downgrade_inbox_0002(op, inbox_name: str = DEFAULT_INBOX_TABLE_NAME) -> None:
    op.drop_index(f"ix_{inbox_name}_received_at", table_name=inbox_name)


def upgrade_claims_0001(op, table_name: str = DEFAULT_CLAIMS_TABLE_NAME) -> None:
    """The `once` claim table. Rows are kept until `SqlClaimStore.purge` removes them."""
    op.create_table(
        table_name,
        sa.Column("claim_key", CLAIM_KEY_TYPE, primary_key=True),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("owner", sa.String(64), nullable=False),
        sa.Column("deadline", _TIMESTAMP, nullable=False),
        sa.Column("digest", sa.String(255), nullable=True),
        sa.Column("result", sa.Text().with_variant(mysql.MEDIUMTEXT(), "mysql"), nullable=True),
        sa.Column("created_at", _TIMESTAMP, nullable=False),
        sa.Column("expires_at", _TIMESTAMP, nullable=False),
    )
    op.create_index(f"ix_{table_name}_created_at", table_name, ["created_at"])


def downgrade_claims_0001(op, table_name: str = DEFAULT_CLAIMS_TABLE_NAME) -> None:
    op.drop_index(f"ix_{table_name}_created_at", table_name=table_name)
    op.drop_table(table_name)
