"""End-to-end checks against a deployed lab. Every test here is billed.

Run only after `scripts/deploy` and with the capability's resources in place:

    uv run pytest -m live tests/live -k capability_1

Tests are ordered: later capabilities rely on state that earlier ones create.
Expected values come from the Confluent Streamhouse quick start. Snapshot
results arrive as strings, so comparisons normalise with `str()`.
"""

from __future__ import annotations

import time

import pytest

from streamhouse_lab import config, sqlfiles, stages
from streamhouse_lab.context import ContextEngine, query_arguments
from streamhouse_lab.flink import FlinkClient

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def cfg() -> config.LabConfig:
    try:
        return config.load()
    except config.ConfigError as exc:
        pytest.skip(f"lab not deployed: {exc}")


@pytest.fixture(scope="module")
def flink(cfg):
    with FlinkClient(cfg) as client:
        yield client


def check(flink: FlinkClient, stage: str, name: str) -> list[dict]:
    sql_file = sqlfiles.parse(sqlfiles.stage(stage).path / "checks" / f"{name}.sql")
    statement = sqlfiles.statement_name("test", stage, name, str(int(time.time())))
    return flink.query(statement, sql_file.statement).records()


def by_key(records: list[dict], key: str) -> dict[str, dict]:
    return {str(r[key]): {k: (None if v is None else str(v)) for k, v in r.items()} for r in records}


def eventually(probe, *, timeout: float = 240.0, interval: float = 10.0):
    """Retry a probe until it returns a truthy value; return (value, seconds waited)."""
    start = time.monotonic()
    while True:
        value = probe()
        if value:
            return value, time.monotonic() - start
        if time.monotonic() - start > timeout:
            pytest.fail(f"condition not met within {timeout:.0f}s; last value: {value!r}")
        time.sleep(interval)


def test_capability_1_sources(flink) -> None:
    stages.apply_stage(flink, sqlfiles.stage("sources"))

    flights = by_key(eventually(lambda: check(flink, "sources", "flight-status"))[0], "flight_id")
    assert set(flights) == {"RA417", "RA210", "RA214", "RA226", "RA232"}
    hotels = by_key(check(flink, "sources", "hotel-inventory"), "hotel")
    assert hotels["Airport Bayfront Inn"]["available_rooms"] == "0"
    itineraries = by_key(check(flink, "sources", "passenger-itineraries"), "passenger_id")
    assert itineraries["P-003"]["connecting_flight_id"] is None


def test_capability_2_transform_join(flink) -> None:
    rows = by_key(check(flink, "transform", "connections"), "passenger_id")
    assert rows["P-001"]["final_destination"] == "SEA"
    assert rows["P-003"]["final_destination"] == "SFO"
    assert rows["P-003"]["connection_minutes"] is None


def passenger_risk(flink) -> dict[str, str]:
    return {
        k: v["risk"]
        for k, v in by_key(check(flink, "passenger-state", "passenger-state"), "passenger_id").items()
    }


def test_capability_3_passenger_state_recomputes_on_delay(flink) -> None:
    stages.apply_stage(flink, sqlfiles.stage("passenger-state"))
    stages.run_event(flink, "ra417-on-time")

    on_time = {"P-001": "OK", "P-002": "OK", "P-003": "NO_CONNECTION"}
    eventually(lambda: passenger_risk(flink) == on_time and on_time)

    stages.run_event(flink, "ra417-delayed")
    delayed = {"P-001": "AT_RISK", "P-002": "AT_RISK", "P-003": "NO_CONNECTION"}
    _, waited = eventually(lambda: passenger_risk(flink) == delayed and delayed)
    # Coarse upper bound only: includes snapshot-query startup. Phase 4 measures freshness properly.
    print(f"passenger_state reflected the delay within {waited:.0f}s of the event completing")

    minutes = by_key(check(flink, "passenger-state", "passenger-state"), "passenger_id")
    assert minutes["P-001"]["connection_minutes"] == "-50"
    assert minutes["P-002"]["connection_minutes"] == "40"


def test_capability_4_lightning_tables_via_context_engine(cfg) -> None:
    if "passenger_state" not in cfg.lightning_topics:
        pytest.skip("passenger_state is not a Lightning Table yet (terraform lightning_topics)")
    tools, calls = ContextEngine(cfg).run(
        [
            ("listTopics", {}),
            (
                "queryData",
                query_arguments(
                    "passenger_state", "SELECT * FROM passenger_state WHERE risk = 'AT_RISK'", 10
                ),
            ),
        ]
    )
    assert {"listTopics", "getMetadata", "queryData"} <= set(tools)
    assert not any(c.is_error for c in calls), [c.payload for c in calls]
    assert "P-001" in str(calls[1].payload) and "P-002" in str(calls[1].payload)


def test_capability_5_tableflow_history(cfg, flink) -> None:
    stages.apply_stage(flink, sqlfiles.stage("history"))
    expected = {
        "RA214": {"flights": "1", "delayed": "1", "impacted": "64"},
        "RA210": {"flights": "1", "delayed": "1", "impacted": "18"},
        "RA417": {"flights": "4", "delayed": "1", "impacted": "120"},
    }

    def matches():
        rows = by_key(check(flink, "history", "flight-performance"), "flight_id")
        got = {k: {c: v[c] for c in ("flights", "delayed", "impacted")} for k, v in rows.items()}
        return got == expected and got

    eventually(matches)
    if "flight_history" not in cfg.tableflow_topics:
        pytest.skip("result came from Kafka only; Tableflow is not enabled on flight_history yet")

    # The Flink result is identical whether it came from Kafka or Iceberg, so read
    # the Iceberg snapshot directly to prove Tableflow materialized the rows.
    from streamhouse_lab import iceberg

    def iceberg_rows():
        return iceberg.rows(iceberg.load(cfg, "flight_history"))

    records, _ = eventually(iceberg_rows, timeout=1200, interval=30)
    per_flight: dict[str, list[int]] = {}
    for r in records:
        agg = per_flight.setdefault(r["flight_id"], [0, 0, 0])
        agg[0] += 1
        agg[1] += r["delay_minutes"] > 15
        agg[2] += r["affected_passengers"]
    assert {k: [str(x) for x in v] for k, v in per_flight.items()} == {
        k: [v["flights"], v["delayed"], v["impacted"]] for k, v in expected.items()
    }


def test_capability_6_rebooking_agent(cfg, flink) -> None:
    if cfg.context_reader is None or "hotel_inventory" not in cfg.lightning_topics:
        pytest.skip(
            "agent needs the context reader and Lightning Tables on flight_status and hotel_inventory"
        )
    stages.apply_stage(flink, sqlfiles.stage("agent"))
    rows = check(flink, "agent", "rebooking-offers")
    responses = by_key(rows, "passenger_id")
    assert set(responses) >= {"P-001", "P-002"}
    for passenger in ("P-001", "P-002"):
        assert responses[passenger]["agent_status"] == "SUCCESS"
        # The model may add a sentence before the offer line; only the offer is asserted.
        assert "OFFER1=RA226;OFFER2=RA232;HOTEL=Harbor Point Hotel" in responses[passenger]["response"]
