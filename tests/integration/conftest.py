"""Real engines for the integration suite: PostgreSQL 16 and MySQL 8.0, plus LocalStack.

URLs come from the environment. A missing URL FAILS the test, never skips it: a skipped
engine reads as a green suite that never ran on that engine (A14).

    FACTBUS_PG_URL=postgresql+psycopg://factbus:factbus@localhost:5452/factbus
    FACTBUS_MYSQL_URL=mysql+pymysql://factbus:factbus@localhost:3326/factbus
    LOCALSTACK_ENDPOINT=http://localhost:4576

Start them with the repository's compose file: `docker compose up -d --wait`.
"""

from __future__ import annotations

import os
import uuid

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import MetaData, create_engine

from factbus.adapters import (
    SqlInbox,
    SqlOutboxStore,
    applied_versions_table,
    downgrade_0001,
    downgrade_inbox_0001,
    downgrade_inbox_0002,
    inbox_table,
    outbox_table,
    upgrade_0001,
    upgrade_inbox_0001,
    upgrade_inbox_0002,
)
from tests.support import ManualClock

ENGINES = {"postgres": "FACTBUS_PG_URL", "mysql": "FACTBUS_MYSQL_URL"}


def _url(variable: str) -> str:
    value = os.environ.get(variable)
    if not value:
        pytest.fail(f"{variable} is not set; the integration suite never skips an engine")
    return value


@pytest.fixture(params=sorted(ENGINES))
def engine(request):
    eng = create_engine(_url(ENGINES[request.param]), pool_size=4, max_overflow=4)
    yield eng
    eng.dispose()


@pytest.fixture
def migrated(engine):
    """A fresh outbox table created by migration 0001, dropped by its downgrade."""
    name = f"ob_{uuid.uuid4().hex[:10]}"
    with engine.begin() as connection:
        upgrade_0001(Operations(MigrationContext.configure(connection)), name)
    table = outbox_table(MetaData(), name)
    yield engine, table
    with engine.begin() as connection:
        downgrade_0001(Operations(MigrationContext.configure(connection)), name)


@pytest.fixture
def store(migrated):
    engine, table = migrated
    clock = ManualClock()
    return SqlOutboxStore(engine, table, clock), clock


@pytest.fixture
def inbox_world(engine):
    """Fresh inbox and applied-version tables created by inbox migration 0001."""
    suffix = uuid.uuid4().hex[:10]
    names = (f"ib_{suffix}", f"av_{suffix}")
    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        upgrade_inbox_0001(operations, *names)
        upgrade_inbox_0002(operations, names[0])
    metadata = MetaData()
    inbox, versions = inbox_table(metadata, names[0]), applied_versions_table(metadata, names[1])
    yield engine, SqlInbox(engine, inbox, versions, ManualClock()), inbox, versions
    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        downgrade_inbox_0002(operations, names[0])
        downgrade_inbox_0001(operations, *names)


@pytest.fixture
def localstack_endpoint() -> str:
    return _url("LOCALSTACK_ENDPOINT")
