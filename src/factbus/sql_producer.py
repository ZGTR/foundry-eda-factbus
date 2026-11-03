"""A SQL producer in one object: stage in the caller's transaction, publish after commit,
sweep what was missed.

Every adopter of the outbox used to write the same glue (the first adopter's was 487
lines across eight files): insert the row on the caller's session, remember which rows
the transaction staged, publish them once the transaction commits, forget them if it
rolls back, and run a sweeper on a schedule. This module is that glue, once.

    producer = SqlProducer.connect(engine, bus_name="orders", region_name="eu-west-1")
    producer.attach()                                  # once, at startup

    with Session(engine) as session, session.begin():
        session.add(order)
        producer.stage(session, Event(type="order.placed", source="shop.orders",
                                      subject=str(order.id), data={"total": order.total}))
    # committed: published now; anything missed goes out on the next sweep

    def sweep_handler(event, context):                 # EventBridge Scheduler, every minute
        return producer.sweep()

PUBLISH AFTER COMMIT. `stage` notes each row in `session.info`. A SQLAlchemy
`after_commit` listener publishes the rows it can READ BACK as committed and unsent from
another connection, so a row staged inside a savepoint that rolled back, or on a Session
whose outer transaction has not committed yet, is not found and not sent; the sweep
publishes it once it is really there. The listener never raises into the caller: the
caller's write has committed, and failing now would report a committed write as failed.

A raw `Connection` (no Session) gets no listener: stage on it, commit, then call
`publish_committed(rows)` yourself, or leave the rows to the sweep.

No boto3 here: the bus adapter is built by `factbus.adapters.eventbridge`.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import MetaData, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from factbus.adapters.emf_metrics import EmfMetrics
from factbus.adapters.eventbridge import build_eventbridge_bus
from factbus.adapters.sql import DEFAULT_TABLE_NAME, SqlOutboxStore, outbox_table
from factbus.application.publish import OnCommitPublisher
from factbus.application.readiness import BusReadiness
from factbus.application.retention import purge_sent_rows
from factbus.application.sweep import Sweeper
from factbus.domain import OutboxRow, RowState
from factbus.envelope import Event
from factbus.ports import Clock, EventBus, Metrics

__all__ = ["SqlProducer"]

log = logging.getLogger(__name__)

PUBLISH_ERROR_METRIC = "OutboxPublishError"


def _utc_now() -> datetime:
    return datetime.now(UTC)


class SqlProducer:
    def __init__(
        self,
        *,
        engine: Engine,
        bus: EventBus,
        table_name: str = DEFAULT_TABLE_NAME,
        metrics: Metrics | None = None,
        clock: Clock = _utc_now,
        publish_on_commit: bool = True,
    ) -> None:
        self.engine = engine
        self._clock = clock
        self._metrics = metrics or EmfMetrics(sys.stdout, namespace="factbus", service="producer")
        self.table = outbox_table(MetaData(), table_name)
        self.store = SqlOutboxStore(engine, self.table, clock)
        self.readiness = BusReadiness(bus, clock, self._metrics)
        self._publisher = OnCommitPublisher(
            bus=bus, store=self.store, readiness=self.readiness, metrics=self._metrics, clock=clock
        )
        self._sweeper = Sweeper(
            bus=bus, store=self.store, readiness=self.readiness, metrics=self._metrics, clock=clock
        )
        self._publish_on_commit = publish_on_commit
        self._staged_key = f"factbus.staged.{table_name}"
        self._attached = False

    @classmethod
    def connect(
        cls,
        engine: Engine,
        *,
        bus_name: str,
        region_name: str,
        endpoint_url: str | None = None,
        service: str = "producer",
        **options: Any,
    ) -> SqlProducer:
        """Build the EventBridge adapter and the producer. factbus reads no environment:
        `region_name` is required, and `endpoint_url` is set locally (LocalStack) and left
        None in AWS."""
        if not region_name:
            raise ValueError("region_name is required")
        bus = build_eventbridge_bus(
            bus_name=bus_name,
            region_name=region_name,
            endpoint_url=endpoint_url,
        )
        options.setdefault("metrics", EmfMetrics(sys.stdout, namespace="factbus", service=service))
        return cls(engine=engine, bus=bus, **options)

    # -- staging ---------------------------------------------------------------------

    def stage(self, session_or_connection: Any, event_: Event | dict) -> OutboxRow:
        """Insert the event's outbox row in the caller's transaction. Never begins or
        commits; database errors propagate so the caller's transaction rolls back."""
        envelope = event_.to_envelope() if isinstance(event_, Event) else self._dated(event_)
        row = self.store.add(session_or_connection, envelope)
        info = getattr(session_or_connection, "info", None)
        if isinstance(session_or_connection, Session) and info is not None:
            info.setdefault(self._staged_key, []).append(row)
        return row

    def _dated(self, envelope: dict) -> dict:
        """A dict envelope without its own timestamp gets one now, at staging: consumers
        refuse an undated envelope, because its age (and so its replay safety) is unknown.
        Stamped here, never at publish, so a republish cannot make an old event look new."""
        if envelope.get("time") or envelope.get("occurred_at"):
            return envelope
        key = "time" if envelope.get("specversion") == "1.0" else "occurred_at"
        return {**envelope, key: self._clock().isoformat()}

    def attach(self) -> None:
        """Publish each Session's staged rows after its commit; forget them on rollback.
        Idempotent. Listens on every Session and acts only on sessions bound to this
        producer's engine."""
        if self._attached:
            return
        event.listen(Session, "after_commit", self._after_commit)
        event.listen(Session, "after_soft_rollback", self._after_soft_rollback)
        self._attached = True

    def detach(self) -> None:
        if not self._attached:
            return
        event.remove(Session, "after_commit", self._after_commit)
        event.remove(Session, "after_soft_rollback", self._after_soft_rollback)
        self._attached = False

    def _after_commit(self, session: Session) -> None:
        # SQLAlchemy also fires `after_commit` when a SAVEPOINT is released. The outer
        # transaction is still open then, and its rows are not committed: keep them for
        # the outer commit instead of dropping them to the sweep.
        if session.get_nested_transaction() is not None:
            return
        if not session.info.get(self._staged_key):
            return
        try:
            # A Session with per-model binds and no default bind raises here; the
            # write has committed, so that must not surface from `commit()`.
            bind = session.get_bind()
        except Exception:
            session.info.pop(self._staged_key, None)
            if self._publish_on_commit:
                log.exception("publish on commit deferred to the sweep")
                self._metrics.count(PUBLISH_ERROR_METRIC)
            return
        # Another producer on another engine may share this table name, and so this
        # staged key: leave its rows for its own listener.
        if getattr(bind, "engine", bind) is not self.engine:
            return
        staged = session.info.pop(self._staged_key, None)
        if staged and self._publish_on_commit:
            self.publish_committed(staged)

    def _after_soft_rollback(self, session: Session, previous_transaction) -> None:
        """Forget staged rows only when the OUTERMOST transaction rolls back.

        A savepoint rollback also fires rollback events. Clearing on it would drop the
        rows the outer transaction still commits, and they would wait for the sweep.
        Keeping them is safe: `publish_committed` reads every row back, so a row staged
        inside the rolled-back savepoint is not found and not sent."""
        if previous_transaction.parent is not None:
            return
        session.info.pop(self._staged_key, None)

    # -- publishing ------------------------------------------------------------------

    def publish_committed(self, staged: Sequence[OutboxRow]) -> list[int]:
        """Publish the rows among `staged` that read back committed and unsent. Returns
        the sent row ids. Never raises: a miss is left to the sweep."""
        try:
            rows = []
            for fact in staged:
                row = self.store.row(fact.row_id)
                if row is not None and row.event_id == fact.event_id and row.state is RowState.UNSENT:
                    rows.append(row)
            return list(self._publisher.publish_on_commit(rows).sent)
        except Exception:
            log.exception("publish on commit deferred to the sweep")
            self._metrics.count(PUBLISH_ERROR_METRIC)
            return []

    def sweep(self) -> dict[str, Any]:
        """One sweeper run, for a scheduled Lambda. Also purges sent rows past retention."""
        report = self._sweeper.run()
        purged = purge_sent_rows(store=self.store, clock=self._clock)
        return {
            "bus_confirmed": report.bus_confirmed,
            "examined": report.examined,
            "sent": len(report.sent),
            "failed": len(report.failed),
            "retry": len(report.retry),
            "purged": purged,
        }

    def sweep_handler(self) -> Callable[[Any, Any], dict[str, Any]]:
        """A Lambda handler for the scheduled sweep."""

        def handler(_event: Any, _context: Any) -> dict[str, Any]:
            return self.sweep()

        return handler
