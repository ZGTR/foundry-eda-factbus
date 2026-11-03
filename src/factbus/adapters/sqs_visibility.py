"""The only `ChangeMessageVisibility` call in the kit.

A consumer sets a failed message's visibility to its backoff wait, so the next receive
comes after 30 s, 2 m, 10 m ... rather than after the queue's default visibility.

THE RETRY POLICY IS WRITTEN, NOT INHERITED, as in `adapters/eventbridge.py`. Here two
attempts are allowed rather than one: `ChangeMessageVisibility` on one receipt handle
is idempotent (setting the same timeout twice has one effect), so an SDK re-send cannot
duplicate anything, and a second attempt saves a receive when the first hits a blip.
A failure after both is `VisibilityUnavailable`, and the consumer still reports the item
(C0b-19).

Construction happens in `build_sqs_visibility`, called by a composition root.
"""

from __future__ import annotations

import boto3
import botocore.exceptions
from botocore.config import Config

from factbus.ports import VisibilityUnavailable

__all__ = ["SQS_CLIENT_CONFIG", "SqsVisibility", "build_sqs_visibility"]

SQS_CLIENT_CONFIG = Config(
    retries={"total_max_attempts": 2, "mode": "standard"},
    connect_timeout=2,
    read_timeout=5,
    # factbus reads no environment, and neither may its clients: without this, boto3
    # takes AWS_ENDPOINT_URL (or a config-file endpoint) when `endpoint_url` is None.
    ignore_configured_endpoint_urls=True,
)


class SqsVisibility:
    def __init__(self, client, queue_url: str) -> None:
        if not queue_url:
            raise ValueError("queue_url is required")
        self._client = client
        self._queue_url = queue_url

    @property
    def queue_url(self) -> str:
        return self._queue_url

    def change_visibility(self, receipt_handle: str, seconds: int) -> None:
        try:
            self._client.change_message_visibility(
                QueueUrl=self._queue_url, ReceiptHandle=receipt_handle, VisibilityTimeout=seconds
            )
        except (botocore.exceptions.ClientError, botocore.exceptions.BotoCoreError) as error:
            raise VisibilityUnavailable(str(error)) from error


def build_sqs_visibility(
    *,
    queue_url: str,
    region_name: str,
    endpoint_url: str | None = None,
    session: boto3.session.Session | None = None,
) -> SqsVisibility:
    """Build the adapter. `endpoint_url` is set locally (LocalStack) and left None in AWS,
    where the function role's credentials and the region select the real queue. The
    queue URL comes from service config (the IaC module outputs it)."""
    client = (session or boto3.session.Session()).client(
        "sqs",
        region_name=region_name,
        endpoint_url=endpoint_url,
        config=SQS_CLIENT_CONFIG,
    )
    return SqsVisibility(client, queue_url)
