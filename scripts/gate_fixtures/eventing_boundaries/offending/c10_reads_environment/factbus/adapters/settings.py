import os
from os import getenv


def region():
    return os.environ.get("AWS_REGION")  # GATE_MUST_FLAG_THIS clause 10


def endpoint():
    return getenv("FACTBUS_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT_URL")  # GATE_MUST_FLAG_THIS clause 10 (twice, plus the import)
