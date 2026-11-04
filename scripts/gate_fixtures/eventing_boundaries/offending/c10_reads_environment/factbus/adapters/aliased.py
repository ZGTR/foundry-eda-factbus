import os as settings
from os import environ as env


def region():
    return settings.getenv("AWS_REGION")  # GATE_MUST_FLAG_THIS clause 10 (through an alias)


def table():
    return env["FACTBUS_INBOX_TABLE"]  # flagged at the import above
