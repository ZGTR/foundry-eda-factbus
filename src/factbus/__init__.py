"""factbus: event-driven services on AWS out of the box.

A producer writes facts to an outbox in its own transaction and factbus publishes them to
an EventBridge bus; a consumer reads its own SQS queue and factbus applies each event
once; a side effect that must not repeat goes through `once`.

    from factbus import Event, SqlProducer, consumer, once

The public names are imported lazily, so a DynamoDB-only consumer never imports
SQLAlchemy. The exception is `factbus.once`, which needs only the standard library: it
loads with the package, so the `once` function is bound before anything can import the
`factbus.once` submodule over it. The layers underneath (`factbus.domain`, `factbus.application`,
`factbus.adapters`, `factbus.ports`) stay importable for composition roots that wire their
own pieces.

Routing and dedupe read three envelope keys: CloudEvents 1.0 `id`, `type` and `source`
(see `factbus.envelope`), or the legacy `event_id`, `type` and `producer`.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from factbus.once import CertainlyNotSent, KeyReused, StillPending, Unknown, once

__version__ = "0.1.0a1"

_EXPORTS = {
    "Event": "factbus.envelope",
    "SqlProducer": "factbus.sql_producer",
    "consumer": "factbus.consumer_handler",
    "once": "factbus.once",
    "Unknown": "factbus.once",
    "StillPending": "factbus.once",
    "KeyReused": "factbus.once",
    "CertainlyNotSent": "factbus.once",
}

__all__ = sorted(_EXPORTS)

if TYPE_CHECKING:
    from factbus.consumer_handler import consumer
    from factbus.envelope import Event
    from factbus.sql_producer import SqlProducer


def __getattr__(name: str) -> Any:
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module 'factbus' has no attribute {name!r}")
    value = getattr(import_module(module), name)
    globals()[name] = value
    return value
