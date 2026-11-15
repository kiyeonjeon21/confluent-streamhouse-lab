"""Creates the Flink connection that lets the rebooking agent call the Real-Time Context Engine.

Terraform's confluent_flink_connection does not accept the MCP_SERVER type
(provider 2.95.0), so this follows the quick start and uses the Confluent CLI.
The CLI takes the secret as an argument: it is visible in the local process
table for the second the command runs, and it is never echoed or logged here.
The connection lives in the lab environment and is deleted with it.
"""

from __future__ import annotations

import json
import subprocess

from streamhouse_lab.config import LabConfig
from streamhouse_lab.context import reader_key
from streamhouse_lab.redact import scrub

CONNECTION_NAME = "rtce-connection"


class ConnectionError_(RuntimeError):
    pass


def _scope(config: LabConfig) -> list[str]:
    return ["--cloud", config.cloud, "--region", config.cloud_region, "--environment", config.environment_id]


def describe(config: LabConfig) -> dict | None:
    proc = subprocess.run(
        ["confluent", "flink", "connection", "describe", CONNECTION_NAME, *_scope(config), "-o", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return None
    return json.loads(proc.stdout)


def create(config: LabConfig) -> dict:
    existing = describe(config)
    if existing is not None:
        return existing
    key = reader_key(config)
    argv = [
        "confluent", "flink", "connection", "create", CONNECTION_NAME,
        *_scope(config),
        "--type", "mcp_server",
        "--endpoint", config.context_mcp_endpoint,
        "--username", key.key,
        "--password", key.secret.reveal(),
        "--transport-type", "STREAMABLE_HTTP",
        "-o", "json",
    ]  # fmt: skip
    proc = subprocess.run(argv, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise ConnectionError_(f"connection create failed: {scrub(proc.stderr.strip(), key)}")
    created = describe(config)
    if created is None:
        raise ConnectionError_("connection create reported success but describe found nothing")
    return created


def delete(config: LabConfig) -> bool:
    proc = subprocess.run(
        ["confluent", "flink", "connection", "delete", CONNECTION_NAME, *_scope(config), "--force"],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode == 0
