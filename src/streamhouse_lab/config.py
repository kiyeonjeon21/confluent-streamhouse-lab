"""Lab configuration, read from Terraform outputs.

Terraform state is the single source of truth for IDs and credentials, so
nothing is copied into a second secrets file. Sensitive outputs are wrapped in
`Secret` as soon as they are parsed and are never written anywhere.
"""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from streamhouse_lab.redact import ApiKey, Secret

REPO_ROOT = Path(__file__).resolve().parents[2]
TERRAFORM_DIR = REPO_ROOT / "terraform"
SQL_DIR = REPO_ROOT / "sql"

OutputReader = Callable[[], dict]


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class LabConfig:
    organization_id: str
    environment_id: str
    cloud_region: str
    kafka_cluster_id: str
    kafka_cluster_type: str
    flink_catalog: str
    flink_database: str
    flink_compute_pool_id: str
    flink_rest_endpoint: str
    operator_id: str
    operator_flink: ApiKey
    context_mcp_endpoint: str
    context_reader: ApiKey | None
    lightning_topics: tuple[str, ...]
    tableflow_topics: tuple[str, ...]
    tableflow_catalog_endpoint: str | None = None
    operator_tableflow: ApiKey | None = None
    kafka_bootstrap: str | None = None
    operator_kafka: ApiKey | None = None
    schema_registry_endpoint: str | None = None
    operator_schema_registry: ApiKey | None = None

    @property
    def cloud(self) -> str:
        return "aws"

    @property
    def lightning_query_endpoint(self) -> str:
        return f"https://sql.{self.cloud_region}.{self.cloud}.confluent.cloud/query/v1"


OUTPUTS_FILE_ENV = "LAB_TERRAFORM_OUTPUTS"


def terraform_outputs(terraform_dir: Path = TERRAFORM_DIR) -> dict:
    """Run `terraform output -json`. The JSON holds secrets, so it stays in memory.

    On a client without the Terraform state (the in-region benchmark host), set
    LAB_TERRAFORM_OUTPUTS to a copy of that JSON, readable only by its owner."""
    copy = os.environ.get(OUTPUTS_FILE_ENV)
    if copy:
        path = Path(copy)
        if path.stat().st_mode & 0o077:
            raise ConfigError(f"{OUTPUTS_FILE_ENV} file holds secrets; chmod 600 it first")
        return json.loads(path.read_text())
    try:
        proc = subprocess.run(
            ["terraform", f"-chdir={terraform_dir}", "output", "-json"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise ConfigError("terraform is not installed or not on PATH") from exc
    if proc.returncode != 0:
        # stderr from `terraform output` never contains output values.
        raise ConfigError(f"terraform output failed: {proc.stderr.strip()}")
    outputs = json.loads(proc.stdout or "{}")
    if not outputs:
        raise ConfigError("Terraform has no outputs yet. Deploy the platform first (scripts/deploy).")
    return outputs


def load(read_outputs: OutputReader = terraform_outputs) -> LabConfig:
    raw = read_outputs()

    def value(name: str, required: bool = True):
        entry = raw.get(name)
        if entry is None or entry.get("value") is None:
            if required:
                raise ConfigError(f"Terraform output {name!r} is missing. Re-run scripts/deploy.")
            return None
        return entry["value"]

    def optional_key(prefix: str) -> ApiKey | None:
        key = value(f"{prefix}_api_key", required=False)
        secret = value(f"{prefix}_api_secret", required=False)
        return ApiKey(key, Secret(secret)) if key and secret else None

    return LabConfig(
        organization_id=value("organization_id"),
        environment_id=value("environment_id"),
        cloud_region=value("cloud_region"),
        kafka_cluster_id=value("kafka_cluster_id"),
        kafka_cluster_type=value("kafka_cluster_type"),
        flink_catalog=value("flink_catalog"),
        flink_database=value("flink_database"),
        flink_compute_pool_id=value("flink_compute_pool_id"),
        flink_rest_endpoint=value("flink_rest_endpoint"),
        operator_id=value("operator_id"),
        operator_flink=ApiKey(value("operator_flink_api_key"), Secret(value("operator_flink_api_secret"))),
        context_mcp_endpoint=value("context_mcp_endpoint"),
        context_reader=optional_key("context_reader"),
        lightning_topics=tuple(value("lightning_topics", required=False) or ()),
        tableflow_topics=tuple(value("tableflow_topics", required=False) or ()),
        tableflow_catalog_endpoint=value("tableflow_catalog_endpoint", required=False),
        operator_tableflow=optional_key("operator_tableflow"),
        kafka_bootstrap=value("kafka_bootstrap", required=False),
        operator_kafka=optional_key("operator_kafka"),
        schema_registry_endpoint=value("schema_registry_endpoint", required=False),
        operator_schema_registry=optional_key("operator_schema_registry"),
    )
