"""Reads Tableflow's Iceberg tables directly through the Tableflow Iceberg REST Catalog.

This bypasses Flink entirely, so it shows what is in the Iceberg snapshot, as
opposed to a Flink snapshot query that also reads the Kafka tail.

Authentication is OAuth2 client credentials with a Tableflow API key
(`key:secret`). For Confluent Managed Storage the catalog vends temporary S3
credentials with each table load; they stay inside pyiceberg's FileIO.
Reference: docs.confluent.io/cloud/current/topics/tableflow/get-started/quick-start-managed-storage.html
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.table import Table

from streamhouse_lab.config import ConfigError, LabConfig

# Tableflow adds these to every table; the default naming scheme prefixes them with $$.
METADATA_PREFIX = "$$"


@dataclass
class SnapshotInfo:
    snapshot_id: int
    committed_at: dt.datetime
    operation: str | None


def catalog(config: LabConfig) -> RestCatalog:
    if not config.tableflow_catalog_endpoint or config.operator_tableflow is None:
        raise ConfigError("No Tableflow API key. Set tableflow_topics in terraform.tfvars and redeploy.")
    key = config.operator_tableflow
    endpoint = config.tableflow_catalog_endpoint.rstrip("/")
    return RestCatalog(
        "tableflow",
        uri=endpoint,
        credential=f"{key.key}:{key.secret.reveal()}",
        # The Iceberg REST spec's token path. Set explicitly: pyiceberg's implicit
        # fallback is deprecated and slated for removal.
        **{"oauth2-server-uri": f"{endpoint}/v1/oauth/tokens"},
    )


def load(config: LabConfig, table: str) -> Table:
    # Tableflow puts each cluster's tables in a namespace named after the cluster ID.
    return catalog(config).load_table((config.kafka_cluster_id, table))


def snapshots(table: Table) -> list[SnapshotInfo]:
    return [
        SnapshotInfo(
            snapshot_id=s.snapshot_id,
            committed_at=dt.datetime.fromtimestamp(s.timestamp_ms / 1000, dt.UTC),
            operation=str(s.summary.operation.value) if s.summary else None,
        )
        for s in table.snapshots()
    ]


def business_columns(table: Table) -> list[str]:
    return [f.name for f in table.schema().fields if not f.name.startswith(METADATA_PREFIX)]


def rows(table: Table, columns: list[str] | None = None, *, with_offset: bool = True) -> list[dict]:
    """Every row in the current snapshot, ordered by Kafka offset."""
    fields = list(columns or business_columns(table))
    if with_offset:
        fields.append(f"{METADATA_PREFIX}offset")
    arrow = table.scan(selected_fields=tuple(fields)).to_arrow()
    if with_offset:
        arrow = arrow.sort_by(f"{METADATA_PREFIX}offset")
    return arrow.to_pylist()
