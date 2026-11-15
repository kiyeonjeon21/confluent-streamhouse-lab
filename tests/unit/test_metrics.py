"""Metrics API client, offline."""

from __future__ import annotations

import json

import httpx
import pytest

from streamhouse_lab import metrics
from tests.conftest import fake_outputs


def test_query_body_filters_by_cluster_and_groups_by_topic() -> None:
    body = metrics.query_body("scanned_bytes", "lkc-1", "2026-11-15T00:00:00Z", "2026-11-15T01:00:00Z")
    assert body["aggregations"] == [{"metric": "io.confluent.rtce/compute_utilization/dp_scanned_bytes"}]
    assert body["filter"] == {"field": "resource.kafka.id", "op": "EQ", "value": "lkc-1"}
    assert body["group_by"] == ["metric.topic"]
    assert body["intervals"] == ["2026-11-15T00:00:00Z/2026-11-15T01:00:00Z"]


def test_usage_sums_each_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    from streamhouse_lab import config

    monkeypatch.setenv("CONFLUENT_CLOUD_API_KEY", "k")
    monkeypatch.setenv("CONFLUENT_CLOUD_API_SECRET", "s")
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append(body["aggregations"][0]["metric"])
        points = [
            {"metric.topic": "bench_passenger_state", "value": 1000},
            {"metric.topic": "bench_passenger_state", "value": 500},
            {"metric.topic": "flight_status", "value": 7},
        ]
        return httpx.Response(200, json={"data": points})

    cfg = config.load(fake_outputs)
    usage = metrics.usage(cfg, "a", "b", transport=httpx.MockTransport(handler))
    assert usage["bench_passenger_state"] == {"scanned_bytes": 1500.0, "ccu_minutes": 1500.0}
    assert usage["flight_status"]["scanned_bytes"] == 7.0
    assert len(seen) == 2


def test_usage_needs_the_cloud_key(monkeypatch: pytest.MonkeyPatch) -> None:
    from streamhouse_lab import config

    monkeypatch.delenv("CONFLUENT_CLOUD_API_KEY", raising=False)
    with pytest.raises(config.ConfigError, match="CONFLUENT_CLOUD_API_KEY"):
        metrics.usage(config.load(fake_outputs), "a", "b")
