#!/usr/bin/env python3
"""End-to-end check of examples/product against an applied stack (LocalStack by default).

Run after `tofu -chdir=examples/product apply -var localstack_endpoint=...` (or
`make e2e-product`). It plays the product's application: a SQL database holding the
product's own `orders` table and the factbus outbox, and `SqlProducer` publishing after
commit. Each arm prints PASS/FAIL rows; any FAIL exits 1:

1. staged:     an order and its `order.placed` fact commit in ONE transaction, and the
               outbox row is marked sent by the after-commit publish (no sweep).
2. consumed:   the consumer's Lambda applies that fact: exactly one ledger item and one
               inbox record for its id, and the queue drains.
3. redelivered: the same CloudEvents id put on the bus again is not applied twice; a
               control fact staged after it is applied and the queue drains.
4. rollback:   an order whose transaction rolls back leaves no order, no outbox row and
               no ledger item; the committed control order staged right after it is applied,
               so its absence is not mere lateness.

The database is `--db-url` (default: $FACTBUS_PG_URL, the compose PostgreSQL), else a
throwaway SQLite file.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    event,
    select,
)
from sqlalchemy.orm import Session

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from factbus import Event, SqlProducer


def orders_table(run_id: str) -> Table:
    """The product's own table, one per run, so concurrent runs never drop each other's."""
    return Table(
        f"e2e_product_orders_{run_id}",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("item", String(50), nullable=False),
    )


def tofu_outputs(tf_dir: str) -> dict[str, Any]:
    raw = subprocess.run(
        ["tofu", f"-chdir={tf_dir}", "output", "-json"], check=True, capture_output=True, text=True
    ).stdout
    return {name: value["value"] for name, value in json.loads(raw).items()}


def wait_for(predicate, timeout: float, interval: float = 1.0) -> tuple[bool, float]:
    start = time.monotonic()
    while True:
        if predicate():
            return True, time.monotonic() - start
        if time.monotonic() - start > timeout:
            return False, time.monotonic() - start
        time.sleep(interval)


def make_engine(url: str | None):
    if url:
        return create_engine(url)
    path = Path(tempfile.mkdtemp(prefix="factbus-product-")) / "product.db"
    engine = create_engine(f"sqlite:///{path}")

    # The documented pysqlite recipe: let SQLAlchemy issue BEGIN, so transactions are real.
    @event.listens_for(engine, "connect")
    def _connect(dbapi_connection, _record):
        dbapi_connection.isolation_level = None

    @event.listens_for(engine, "begin")
    def _begin(connection):
        connection.exec_driver_sql("BEGIN")

    return engine


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tf-dir", default=str(REPO / "examples" / "product"))
    parser.add_argument(
        "--endpoint",
        default=os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4576"),
        help="LocalStack URL; '' for real AWS",
    )
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--db-url", default=os.environ.get("FACTBUS_PG_URL"))
    parser.add_argument("--timeout", type=float, default=90.0)
    args = parser.parse_args()

    endpoint = args.endpoint or None
    out = tofu_outputs(args.tf_dir)
    consumer = out["consumer"]
    kwargs: dict[str, Any] = {"region_name": args.region}
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    ddb = boto3.resource("dynamodb", **kwargs)
    sqs = boto3.client("sqs", **kwargs)
    events = boto3.client("events", **kwargs)
    logs = boto3.client("logs", **kwargs)
    ledger = ddb.Table(consumer["ledger_table"])
    inbox = ddb.Table(consumer["inbox_table"])

    failures: list[str] = []

    def check(name: str, passed: bool, detail: str) -> None:
        print(f"{'PASS' if passed else 'FAIL'}  {name:<10} {detail}", flush=True)
        if not passed:
            failures.append(name)

    def ledger_count(event_id: str) -> int:
        response = ledger.query(KeyConditionExpression=Key("pk").eq(event_id), ConsistentRead=True)
        return len(response["Items"])

    def in_inbox(event_id: str) -> bool:
        response = inbox.get_item(Key={"pk": f"inbox#{event_id}", "sk": "inbox"}, ConsistentRead=True)
        return "Item" in response

    def queue_depth() -> int:
        attrs = sqs.get_queue_attributes(
            QueueUrl=consumer["queue_url"],
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )["Attributes"]
        return int(attrs["ApproximateNumberOfMessages"]) + int(attrs["ApproximateNumberOfMessagesNotVisible"])

    def duplicate_lines(since_ms: int) -> int:
        """The consumer's own EMF lines counting a skipped duplicate since `since_ms`."""
        group = f"/aws/lambda/{consumer['function_name']}"
        count, token = 0, None
        while True:
            request: dict[str, Any] = {"logGroupName": group, "startTime": since_ms}
            if token:
                request["nextToken"] = token
            try:
                response = logs.filter_log_events(**request)
            except logs.exceptions.ResourceNotFoundException:
                return 0
            count += sum(1 for e in response["events"] if '"ConsumerDuplicates"' in e["message"])
            token = response.get("nextToken")
            if not token:
                return count

    engine = make_engine(args.db_url)
    # A per-run outbox table, so reruns against the same compose database never see an
    # earlier run's rows.
    run_id = uuid.uuid4().hex[:8]
    ORDERS = orders_table(run_id)
    producer = SqlProducer.connect(
        engine,
        bus_name=out["bus_name"],
        region_name=args.region,
        endpoint_url=endpoint,
        table_name=f"factbus_outbox_e2e_{run_id}",
        service="product-e2e",
    )
    producer.table.metadata.create_all(engine)
    ORDERS.metadata.create_all(engine)
    producer.attach()

    def placed(order_id: int, item: str) -> Event:
        return Event(
            id=f"order-{run_id}-{order_id}",
            type=out["event_type"],
            source=out["event_source"],
            subject=str(order_id),
            data={"item": item},
        )

    try:
        base = int(time.time()) % 1_000_000 * 10

        # 1. staged: the order and its fact commit together; publish happens after commit.
        first = placed(base + 1, "book")
        with Session(engine) as session, session.begin():
            session.execute(ORDERS.insert().values(id=base + 1, item="book"))
            row = producer.stage(session, first)
        sent = producer.store.row(row.row_id)
        check(
            "staged",
            sent is not None and sent.sent_at is not None,
            f"outbox row {row.row_id} sent_at={getattr(sent, 'sent_at', None)} (after-commit publish, no sweep)",
        )

        # 2. consumed: the consumer applies it exactly once.
        ok, waited = wait_for(lambda: ledger_count(first.id) >= 1, args.timeout)
        time.sleep(2)  # a second apply, if any, would land in this window
        count = ledger_count(first.id)
        check("consumed", ok and count == 1, f"ledger items for {first.id}: {count} after {waited:.1f}s")
        check("inbox", in_inbox(first.id), f"inbox record for {first.id}")
        drained, _ = wait_for(lambda: queue_depth() == 0, args.timeout)
        check("drained", drained, f"queue depth {queue_depth()}")

        # 3. redelivered: the same CloudEvents id put on the bus again (a replay, or the
        # sweep re-sending a row whose first publish was not acknowledged) changes nothing.
        # A control event put after it must be applied, so the copy was consumed, not late.
        since_ms = int(time.time() * 1000) - 1000
        response = events.put_events(
            Entries=[
                {
                    "EventBusName": out["bus_name"],
                    "Source": first.source,
                    "DetailType": first.type,
                    "Detail": json.dumps(first.to_envelope()),
                }
            ]
        )
        dup_control = placed(base + 4, "cup")
        with Session(engine) as session, session.begin():
            session.execute(ORDERS.insert().values(id=base + 4, item="cup"))
            producer.stage(session, dup_control)
        ok, _ = wait_for(lambda: ledger_count(dup_control.id) == 1, args.timeout)
        # The copy itself must have reached the consumer, which records it as a duplicate;
        # an empty queue alone could mean it is still in flight.
        seen, _ = wait_for(lambda: duplicate_lines(since_ms) >= 1, args.timeout, interval=2.0)
        drained, _ = wait_for(lambda: queue_depth() == 0, args.timeout)
        count = ledger_count(first.id)
        check(
            "redelivered",
            not response.get("FailedEntryCount") and ok and seen and drained and count == 1,
            f"same id {first.id} put again: consumer logged ConsumerDuplicates={seen}; "
            f"ledger items {count}; control applied={ok}; drained={drained}",
        )

        # 4. rollback: nothing leaves a rolled-back transaction.
        rolled = placed(base + 2, "lamp")
        try:
            with Session(engine) as session, session.begin():
                session.execute(ORDERS.insert().values(id=base + 2, item="lamp"))
                producer.stage(session, rolled)
                raise RuntimeError("the business write failed after staging")
        except RuntimeError:
            pass
        control = placed(base + 3, "pen")
        with Session(engine) as session, session.begin():
            session.execute(ORDERS.insert().values(id=base + 3, item="pen"))
            producer.stage(session, control)
        ok, _ = wait_for(lambda: ledger_count(control.id) == 1, args.timeout)
        with engine.connect() as connection:
            order_rows = connection.execute(select(ORDERS).where(ORDERS.c.id == base + 2)).all()
            outbox_rows = connection.execute(
                select(producer.table).where(producer.table.c.event_id == rolled.id)
            ).all()
        check(
            "rollback",
            ok and not order_rows and not outbox_rows and ledger_count(rolled.id) == 0,
            f"control applied={ok}; rolled-back order rows={len(order_rows)}, outbox rows={len(outbox_rows)}, "
            f"ledger items={ledger_count(rolled.id)}",
        )
    finally:
        producer.detach()
        try:
            producer.table.drop(engine)
            ORDERS.drop(engine)
        finally:
            engine.dispose()

    print(f"\n{'FAILED: ' + ', '.join(failures) if failures else 'ALL PASS'}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
