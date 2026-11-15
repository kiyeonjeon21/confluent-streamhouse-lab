from __future__ import annotations

import pytest

from streamhouse_lab import config
from streamhouse_lab.redact import ApiKey, Secret

FAKE_SECRET = "fake-secret-value-0123456789abcdef"


def fake_outputs(**overrides) -> dict:
    values = {
        "organization_id": "00000000-0000-0000-0000-000000000000",
        "environment_id": "env-test01",
        "cloud_region": "us-east-1",
        "kafka_cluster_id": "lkc-test01",
        "kafka_cluster_type": "basic",
        "flink_catalog": "river-air-lab",
        "flink_database": "river-air",
        "flink_compute_pool_id": "lfcp-test01",
        "flink_rest_endpoint": "https://flink.us-east-1.aws.confluent.cloud",
        "operator_id": "sa-test01",
        "operator_flink_api_key": "FLINKKEY",
        "operator_flink_api_secret": FAKE_SECRET,
        "context_mcp_endpoint": "https://mcp.us-east-1.aws.confluent.cloud/mcp/v1/context-engine/x",
        "context_reader_api_key": None,
        "context_reader_api_secret": None,
        "lightning_topics": [],
        "tableflow_topics": [],
    }
    values.update(overrides)
    return {k: {"value": v, "sensitive": "secret" in k} for k, v in values.items()}


@pytest.fixture
def lab_config() -> config.LabConfig:
    return config.load(lambda: fake_outputs())


@pytest.fixture
def lab_config_with_context() -> config.LabConfig:
    return config.load(
        lambda: fake_outputs(context_reader_api_key="CTXKEY", context_reader_api_secret=FAKE_SECRET + "-ctx")
    )


@pytest.fixture
def api_key() -> ApiKey:
    return ApiKey("KEY", Secret(FAKE_SECRET))
