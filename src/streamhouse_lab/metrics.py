"""Lightning Tables usage from the Confluent Cloud Metrics API (read-only, not billed).

`dp_scanned_bytes` and `ccu_minutes` are counters, so a sum over an interval is the
total for that interval; grouping by `metric.topic` gives one figure per table.
Reference: docs.confluent.io/cloud/current/lightning-tables/monitor.html

The Metrics API takes a Cloud API key; the lab uses the one Terraform reads from
.env (CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET).
"""

from __future__ import annotations

import os

import httpx

from streamhouse_lab.config import ConfigError, LabConfig

ENDPOINT = "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"
METRICS = {
    "scanned_bytes": "io.confluent.rtce/compute_utilization/dp_scanned_bytes",
    "ccu_minutes": "io.confluent.rtce/compute_utilization/ccu_minutes",
}


def query_body(metric: str, cluster_id: str, start: str, end: str, granularity: str = "PT1M") -> dict:
    return {
        "aggregations": [{"metric": METRICS[metric]}],
        "filter": {"field": "resource.kafka.id", "op": "EQ", "value": cluster_id},
        "granularity": granularity,
        "group_by": ["metric.topic"],
        "intervals": [f"{start}/{end}"],
        "limit": 1000,
    }


def totals(data: list[dict]) -> dict[str, float]:
    """Sum each topic's points over the interval."""
    out: dict[str, float] = {}
    for point in data:
        topic = point.get("metric.topic", "?")
        out[topic] = out.get(topic, 0.0) + float(point.get("value") or 0.0)
    return out


def usage(
    config: LabConfig, start: str, end: str, *, transport: httpx.BaseTransport | None = None
) -> dict[str, dict[str, float]]:
    key, secret = os.environ.get("CONFLUENT_CLOUD_API_KEY"), os.environ.get("CONFLUENT_CLOUD_API_SECRET")
    if not key or not secret:
        raise ConfigError("Set CONFLUENT_CLOUD_API_KEY and CONFLUENT_CLOUD_API_SECRET (see .env.example).")
    result: dict[str, dict[str, float]] = {}
    with httpx.Client(auth=httpx.BasicAuth(key, secret), timeout=30.0, transport=transport) as http:
        for name in METRICS:
            response = http.post(ENDPOINT, json=query_body(name, config.kafka_cluster_id, start, end))
            if not response.is_success:
                raise ConfigError(f"Metrics API {name}: HTTP {response.status_code}: {response.text[:300]}")
            for topic, value in totals(response.json().get("data") or []).items():
                result.setdefault(topic, {})[name] = value
    return result
