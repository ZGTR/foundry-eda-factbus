"""`SqlClaimStore`: the `ClaimStore` port on one SQL table. PostgreSQL 16 and MySQL 8.0.

The same contract as `DynamoClaimStore`, so `factbus.once` behaves identically on
either: a claim row moves `pending` to `done` or `unknown` and never back; a stale
`pending` row resolves to `unknown` without calling; `release` deletes only a `pending`
row its owner still holds. Every write is conditional and a lost condition is `False`.

`claim` is an insert that does nothing when the key exists, written per dialect so a
second claimer never meets a unique-key error (which would abort a PostgreSQL
transaction and fill the log):

- PostgreSQL and SQLite: `INSERT ... ON CONFLICT (claim_key) DO NOTHING`. On PostgreSQL
  a `RETURNING` row says this call wrote the row (psycopg 3 reports no row count for the
  statement); on SQLite the row count does. A second claimer waits on the first
  claimer's uncommitted insert, then does nothing.
- MySQL: `INSERT ... ON DUPLICATE KEY UPDATE claim_key = claim_key`, a no-op that also
  takes the existing row's exclusive lock. The MySQL dialects set `CLIENT_FOUND_ROWS`,
  so the row count cannot tell "inserted" from "already there"; the store reads the
  row's owner back under that lock instead. Owners are random per attempt, so a match
  means this call wrote the row.

When the key exists, the same transaction takes the row over only if it is past
`expires_at` at `now` (the DynamoDB condition `attribute_not_exists OR expires_at <
:now`). Until then the row guards the key, whatever its state.

`purge` deletes rows created before a cutoff in bounded batches, and never a row whose
own `expires_at` is still ahead: a `once` with a retention longer than the purge window
keeps its guard. Each batch is a plain read of keys followed by a delete of exactly
those keys, re-checking both conditions, so a claim written meanwhile is never removed
and no range lock is held against new claims.

Keys and digests are at most 255 characters; a longer one is refused before any I/O
rather than truncated by the engine.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from sqlalchemy import Column, Index, MetaData, String, Table, Text, delete, select, update
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from factbus.adapters.sql import TIMESTAMP, _to_db
from factbus.domain import ROW_RETENTION
from factbus.domain.claims import Claim, ClaimState
from factbus.ports import ClaimStoreError, Clock

__all__ = ["DEFAULT_CLAIMS_TABLE_NAME", "PURGE_BATCH_SIZE", "SqlClaimStore", "claims_table"]

DEFAULT_CLAIMS_TABLE_NAME = "factbus_claims"
KEY_LENGTH = 255
DIGEST_LENGTH = 255
PURGE_BATCH_SIZE = 1000

RESULT_TEXT = Text().with_variant(mysql.MEDIUMTEXT(), "mysql")

#: Keys compare byte for byte. MySQL's default utf8mb4 collation ignores case, so
#: `tenant-A#x` and `tenant-a#x` would share one claim and one cached result, and
#: `utf8mb4_bin` is PAD SPACE (`'k' = 'k '`). `utf8mb4_0900_bin` (MySQL 8.0.17+) is
#: NO PAD and case-sensitive.
#: PostgreSQL and SQLite already compare `=` and primary keys bytewise by default.
CLAIM_KEY_TYPE = String(KEY_LENGTH).with_variant(
    mysql.VARCHAR(KEY_LENGTH, charset="utf8mb4", collation="utf8mb4_0900_bin"), "mysql"
)

_SUPPORTED_DIALECTS = ("postgresql", "mysql", "sqlite")


def claims_table(metadata: MetaData, name: str = DEFAULT_CLAIMS_TABLE_NAME) -> Table:
    return Table(
        name,
        metadata,
        Column("claim_key", CLAIM_KEY_TYPE, primary_key=True),
        Column("state", String(16), nullable=False),
        Column("owner", String(64), nullable=False),
        Column("deadline", TIMESTAMP, nullable=False),
        Column("digest", String(DIGEST_LENGTH), nullable=True),
        Column("result", RESULT_TEXT, nullable=True),
        Column("created_at", TIMESTAMP, nullable=False),
        Column("expires_at", TIMESTAMP, nullable=False),
        Index(f"ix_{name}_created_at", "created_at"),
    )


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _aware(moment: datetime) -> datetime:
    """A stored naive-UTC timestamp, as the timezone-aware value `Claim` holds."""
    return moment.replace(tzinfo=UTC)


class SqlClaimStore:
    def __init__(self, engine: Engine, table: Table | str = DEFAULT_CLAIMS_TABLE_NAME, *, clock: Clock | None = None) -> None:
        if engine.dialect.name not in _SUPPORTED_DIALECTS:
            raise ValueError(f"SqlClaimStore supports {', '.join(_SUPPORTED_DIALECTS)}, not {engine.dialect.name}")
        self._engine = engine
        self._table = claims_table(MetaData(), table) if isinstance(table, str) else table
        self._clock: Callable[[], datetime] = clock or _utc_now

    @property
    def table(self) -> Table:
        return self._table

    def claim(self, claim: Claim, now: datetime) -> bool:
        if len(claim.key) > KEY_LENGTH:
            raise ValueError(f"a claim key is at most {KEY_LENGTH} characters")
        if claim.digest is not None and len(claim.digest) > DIGEST_LENGTH:
            raise ValueError(f"a claim digest is at most {DIGEST_LENGTH} characters")
        t = self._table
        values: dict[str, object] = {
            "state": claim.state.value,
            "owner": claim.owner,
            "deadline": _to_db(claim.deadline),
            "digest": claim.digest,
            "result": claim.result_json,
            "created_at": _to_db(now),
            "expires_at": _to_db(claim.expires),
        }
        try:
            with self._engine.begin() as connection:
                if self._insert_if_absent(connection, claim.key, claim.owner, values):
                    return True
                # The key is held. Take it over only when the holder's row has expired.
                taken = connection.execute(
                    update(t).where(t.c.claim_key == claim.key, t.c.expires_at < _to_db(now)).values(**values)
                )
                return taken.rowcount == 1
        except SQLAlchemyError as error:
            raise ClaimStoreError(str(error)) from error

    def read(self, key: str) -> Claim | None:
        try:
            with self._engine.connect() as connection:
                row = connection.execute(select(self._table).where(self._table.c.claim_key == key)).first()
        except SQLAlchemyError as error:
            raise ClaimStoreError(str(error)) from error
        if row is None:
            return None
        return Claim(
            key=row.claim_key,
            state=ClaimState(row.state),
            owner=row.owner,
            deadline=_aware(row.deadline),
            digest=row.digest,
            expires=_aware(row.expires_at),
            result_json=row.result,
        )

    def settle(self, key: str, owner: str, state: ClaimState, *, result_json: str | None = None) -> bool:
        if state is ClaimState.PENDING:
            raise ValueError("settle moves a claim out of pending, not into it")
        values: dict[str, object] = {"state": state.value}
        if result_json is not None:
            values["result"] = result_json
        return self._conditional(update(self._table).where(*self._held(key, owner)).values(**values))

    def release(self, key: str, owner: str) -> bool:
        return self._conditional(delete(self._table).where(*self._held(key, owner)))

    def purge(self, older_than: timedelta = ROW_RETENTION, *, batch_size: int = PURGE_BATCH_SIZE) -> int:
        """Delete rows created more than `older_than` ago whose `expires_at` has passed.
        Returns the number deleted. Each batch of at most `batch_size` keys is its own
        short transaction."""
        if older_than <= timedelta(0):
            raise ValueError("older_than must be positive")
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        t = self._table
        now = self._clock()
        due = (t.c.created_at < _to_db(now - older_than), t.c.expires_at < _to_db(now))
        deleted = 0
        try:
            while True:
                with self._engine.begin() as connection:
                    keys = [
                        record.claim_key
                        for record in connection.execute(
                            select(t.c.claim_key).where(*due).order_by(t.c.created_at).limit(batch_size)
                        )
                    ]
                    if keys:
                        deleted += connection.execute(delete(t).where(t.c.claim_key.in_(keys), *due)).rowcount
                if len(keys) < batch_size:
                    return deleted
        except SQLAlchemyError as error:
            raise ClaimStoreError(str(error)) from error

    def _held(self, key: str, owner: str) -> tuple:
        t = self._table
        return (t.c.claim_key == key, t.c.owner == owner, t.c.state == ClaimState.PENDING.value)

    def _insert_if_absent(self, connection, key: str, owner: str, values: dict[str, object]) -> bool:
        """True when this call inserted the row for `key`."""
        t = self._table
        dialect = connection.dialect.name
        if dialect == "mysql":
            upsert = mysql.insert(t).values(claim_key=key, **values)
            connection.execute(upsert.on_duplicate_key_update(claim_key=t.c.claim_key))
            holder = connection.execute(
                select(t.c.owner).where(t.c.claim_key == key).with_for_update()
            ).scalar_one_or_none()
            return holder == owner
        if dialect == "postgresql":
            # psycopg 3 reports no row count for this statement, so ask for the row back:
            # `RETURNING` yields one row when this call inserted and none on a conflict.
            guarded = postgresql.insert(t).values(claim_key=key, **values)
            returning = guarded.on_conflict_do_nothing(index_elements=[t.c.claim_key]).returning(t.c.claim_key)
            return connection.execute(returning).first() is not None
        result = connection.execute(
            sqlite.insert(t).values(claim_key=key, **values).on_conflict_do_nothing(index_elements=[t.c.claim_key])
        )
        return result.rowcount == 1

    def _conditional(self, statement) -> bool:
        try:
            with self._engine.begin() as connection:
                return connection.execute(statement).rowcount == 1
        except SQLAlchemyError as error:
            raise ClaimStoreError(str(error)) from error
