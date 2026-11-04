"""The product's consumer: one `order.placed` fact writes one ledger item.

The write goes through `tx`, so it commits in the same DynamoDB transaction as the
factbus inbox record, and a redelivered or replayed copy of the event writes nothing.
This file is all a product writes for a consumer; the queue, retries, DLQs and inbox
come from factbus.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime
from functools import partial

from factbus import Event, consumer
from factbus.adapters.dynamodb_inbox import build_dynamo_inbox

# The function's configuration, read once here: factbus itself reads no environment.
# Lambda sets AWS_REGION; LocalStack sets AWS_ENDPOINT_URL inside its Lambda containers,
# and it is unset in AWS.
_REGION = os.environ["AWS_REGION"]
_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL") or None


@consumer(
    inbox=partial(build_dynamo_inbox, table_name=os.environ["FACTBUS_INBOX_TABLE"],
                  region_name=_REGION, endpoint_url=_ENDPOINT),
    endpoint_url=_ENDPOINT,
    types=["order.placed"],
    service=os.environ.get("CONSUMER_SERVICE", "order-ledger"),
)
def handler(event: Event, tx) -> None:
    tx.put(
        os.environ["LEDGER_TABLE"],
        {
            "pk": event.id,
            "sk": str(uuid.uuid4()),  # unique per apply, so a second apply would show
            "order_id": str(event.subject),
            "item": str(event.data.get("item", "")),
            "source": event.source,
            "applied_at": datetime.now(UTC).isoformat(),
        },
    )
