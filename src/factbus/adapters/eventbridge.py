"""The only `PutEvents` and `DescribeEventBus` calls, and the only boto3 import in the kit.

THE RETRY POLICY IS WRITTEN, NOT INHERITED. `boto3.client("events")` with no `config=`
takes botocore's `standard` mode and re-sends a failed request up to three times inside
the SDK, invisibly: the caller sees one exception for several sends. Here a hidden re-send would be a duplicate on the bus the outbox cannot count, and it
spends a Lambda's time budget before the marks are written. So the client is built with
`total_max_attempts=1`: one call is one request, and the sweeper is the retry layer.
The default was measured in action: a malformed request against LocalStack failed with
"reached max retries: 4" (P10).

Construction happens in `build_eventbridge_bus`, called by a composition root. Nothing
here is built at import time.
"""

from __future__ import annotations

from collections.abc import Sequence

import boto3
import botocore.exceptions
from botocore.config import Config

from factbus.domain import BusEntry, PutEventsResponse
from factbus.ports import BusUnavailable

__all__ = ["EVENTBRIDGE_CLIENT_CONFIG", "EventBridgeBus", "build_eventbridge_bus"]

EVENTBRIDGE_CLIENT_CONFIG = Config(
    retries={"total_max_attempts": 1, "mode": "standard"},
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

_BUS_MISSING_CODES = frozenset({"ResourceNotFoundException"})


class EventBridgeBus:
    def __init__(self, client, bus_name: str) -> None:
        if not bus_name:
            raise ValueError("bus_name is required")
        self._client = client
        self._bus_name = bus_name

    @property
    def bus_name(self) -> str:
        return self._bus_name

    def put_events(self, entries: Sequence[BusEntry]) -> PutEventsResponse:
        request = [
            {
                "EventBusName": self._bus_name,
                "Source": entry.source,
                "DetailType": entry.detail_type,
                "Detail": entry.detail,
            }
            for entry in entries
        ]
        try:
            response = self._client.put_events(Entries=request)
        except botocore.exceptions.ClientError as error:
            code = error.response.get("Error", {}).get("Code", "ClientError")
            raise BusUnavailable(str(error), code=code) from error
        except botocore.exceptions.BotoCoreError as error:
            raise BusUnavailable(str(error), code=type(error).__name__) from error
        return PutEventsResponse(
            failed_entry_count=int(response.get("FailedEntryCount", 0)),
            entry_error_codes=tuple(item.get("ErrorCode") or None for item in response.get("Entries", [])),
        )

    def bus_exists(self) -> bool:
        try:
            self._client.describe_event_bus(Name=self._bus_name)
        except botocore.exceptions.ClientError as error:
            if error.response.get("Error", {}).get("Code") in _BUS_MISSING_CODES:
                return False
            raise BusUnavailable(str(error), code=error.response.get("Error", {}).get("Code", "ClientError")) from error
        except botocore.exceptions.BotoCoreError as error:
            raise BusUnavailable(str(error), code=type(error).__name__) from error
        return True


def build_eventbridge_bus(
    *,
    bus_name: str,
    region_name: str,
    endpoint_url: str | None = None,
    session: boto3.session.Session | None = None,
) -> EventBridgeBus:
    """Build the adapter. `endpoint_url` is set locally (LocalStack) and left None in AWS,
    where the Lambda role's credentials and the region select the real bus."""
    client = (session or boto3.session.Session()).client(
        "events",
        region_name=region_name,
        endpoint_url=endpoint_url,
        config=EVENTBRIDGE_CLIENT_CONFIG,
    )
    return EventBridgeBus(client, bus_name)
