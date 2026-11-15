"""Phase 4 tooling: data generator, serialization, recorder, summaries, and runners."""

from __future__ import annotations

import asyncio
import datetime as dt
import io
import json
import re
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import fastavro
import pytest

from streamhouse_lab import bench, datagen, kafka_io, measure, sqlfiles, summarize
from streamhouse_lab.config import SQL_DIR, ConfigError
from streamhouse_lab.context import ToolCall
from streamhouse_lab.redact import ApiKey, Secret
from tests.conftest import FAKE_SECRET, fake_outputs

SCHEMAS = Path(__file__).parent.parent / "fixtures" / "schemas"


# -- SQL ---------------------------------------------------------------------


def normalized(sql: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"\bbench_", "", sql)).strip()


def test_bench_state_query_matches_passenger_state() -> None:
    original = sqlfiles.parse(SQL_DIR / "03-passenger-state" / "01-create-passenger-state.sql")
    copy = sqlfiles.parse(SQL_DIR / "07-bench" / "03-create-bench-passenger-state.sql")
    assert normalized(copy.statement) == normalized(original.statement)


@pytest.mark.parametrize(
    ("original", "copy"),
    [
        ("01-sources/01-create-flight-status.sql", "07-bench/01-create-bench-flight-status.sql"),
        (
            "01-sources/02-create-passenger-itineraries.sql",
            "07-bench/02-create-bench-passenger-itineraries.sql",
        ),
    ],
)
def test_bench_tables_match_sources(original: str, copy: str) -> None:
    assert normalized(sqlfiles.parse(SQL_DIR / copy).statement) == normalized(
        sqlfiles.parse(SQL_DIR / original).statement
    )


# -- generator ---------------------------------------------------------------


def test_generation_is_deterministic_and_seed_sensitive() -> None:
    a, b, c = datagen.generate(500, 1), datagen.generate(500, 1), datagen.generate(500, 2)
    assert a == b
    assert a != c


def test_generated_ids_are_unique_and_referentially_complete() -> None:
    data = datagen.generate(2_000, 3)
    flight_ids = [r.key["flight_id"] for r in data.flights]
    passenger_ids = [r.key["passenger_id"] for r in data.itineraries]
    assert len(flight_ids) == len(set(flight_ids))
    assert len(passenger_ids) == len(set(passenger_ids)) == 2_000
    for r in data.itineraries:
        assert r.value["inbound_flight_id"] in flight_ids
        assert r.value["connecting_flight_id"] in (None, *flight_ids)
    assert all(not fid.startswith("RA") for fid in flight_ids), "must not collide with quick start flights"


def test_records_yield_flights_before_itineraries() -> None:
    data = datagen.generate(300, 1)
    topics = [r.topic for r in data.records()]
    assert topics == ["bench_flight_status"] * len(data.flights) + ["bench_passenger_itineraries"] * 300


def test_probe_passenger_connects_and_markers_are_unique() -> None:
    data = datagen.generate(10, 5)
    probe = next(r for r in data.itineraries if r.key["passenger_id"] == datagen.PROBE_PASSENGER)
    assert probe.value["connecting_flight_id"] is not None
    flight = datagen.probe_flight(data)
    markers = [datagen.marker_update(flight, k).value["estimated_time"] for k in range(50)]
    assert len(set(markers)) == 50
    assert all(m > flight.value["scheduled_time"] for m in markers)


def test_expected_risk_matches_materialized_table_rules() -> None:
    arrival = dt.datetime(2026, 12, 1, 10, 0)
    flights = [
        datagen.flight_record("IN", "ORD", "SFO", arrival),
        datagen.flight_record("FAST", "SFO", "SEA", arrival + dt.timedelta(minutes=44)),
        datagen.flight_record("EDGE", "SFO", "SEA", arrival + dt.timedelta(minutes=45)),
    ]

    def itinerary(pid, connecting):
        return datagen.Record(
            "bench_passenger_itineraries",
            {"passenger_id": pid},
            {"inbound_flight_id": "IN", "connecting_flight_id": connecting},
        )

    data = datagen.Dataset(flights, [itinerary("a", "FAST"), itinerary("b", "EDGE"), itinerary("c", None)])
    assert datagen.expected_risk(data) == {"OK": 1, "AT_RISK": 1, "NO_CONNECTION": 1}


def test_generated_records_serialize_with_flink_registered_schemas() -> None:
    """Schemas copied from Schema Registry (subjects of the quick start tables, which the
    bench_ tables share in shape). Avro local-timestamp-millis takes naive datetimes."""
    data = datagen.generate(50, 9)
    samples = {"flight_status": data.flights[0], "passenger_itineraries": data.itineraries[0]}
    for table, record in samples.items():
        for part, payload in (("key", record.key), ("value", record.value)):
            schema = fastavro.parse_schema(json.loads((SCHEMAS / f"{table}-{part}.avsc").read_text()))
            buffer = io.BytesIO()
            fastavro.schemaless_writer(buffer, schema, payload)
            buffer.seek(0)
            assert fastavro.schemaless_reader(buffer, schema) == payload


# -- Kafka producer wrapper --------------------------------------------------


def test_producer_config_strips_protocol_and_requires_keys() -> None:
    from streamhouse_lab import config

    missing = config.load(lambda: fake_outputs())
    with pytest.raises(ConfigError, match="enable_bench_clients"):
        kafka_io.producer_config(missing)
    ready = config.load(
        lambda: fake_outputs(
            kafka_bootstrap="SASL_SSL://pkc-x.us-east-1.aws.confluent.cloud:9092",
            operator_kafka_api_key="KAFKAKEY",
            operator_kafka_api_secret=FAKE_SECRET,
        )
    )
    conf = kafka_io.producer_config(ready)
    assert conf["bootstrap.servers"] == "pkc-x.us-east-1.aws.confluent.cloud:9092"
    assert conf["acks"] == "all" and conf["enable.idempotence"] is True
    assert FAKE_SECRET not in repr(ready)


class FakeProducer:
    def __init__(self, fail: bool = False) -> None:
        self.pending: list = []
        self.fail = fail
        self.offset = 0

    def produce(self, topic, key, value, on_delivery):
        self.pending.append((topic, on_delivery))

    def poll(self, _timeout):
        return 0

    def flush(self, _timeout):
        for topic, callback in self.pending:
            message = SimpleNamespace(
                topic=lambda t=topic: t, partition=lambda: 0, offset=lambda o=self.offset: o
            )
            self.offset += 1
            callback("boom" if self.fail else None, message)
        self.pending.clear()
        return 0


def test_bench_producer_returns_acks_in_order() -> None:
    ticks = iter(range(100, 10_000))
    producer = kafka_io.BenchProducer(FakeProducer(), lambda r: (b"k", b"v"), clock=lambda: next(ticks))
    acks = producer.send(datagen.generate(3, 1).itineraries)
    assert [a.offset for a in acks] == [0, 1, 2]
    assert acks[0].acked_ns < acks[-1].acked_ns


def test_bench_producer_raises_on_delivery_failure() -> None:
    producer = kafka_io.BenchProducer(FakeProducer(fail=True), lambda r: (b"k", b"v"))
    with pytest.raises(kafka_io.ProduceError, match="1 failed"):
        producer.send(datagen.generate(1, 1).itineraries)


# -- recorder and summaries --------------------------------------------------


def make_recorder(tmp_path: Path, ticks=None) -> measure.Recorder:
    clock = iter(ticks or range(0, 10**12, 10**6))
    return measure.Recorder(
        "e0",
        {"n": 1},
        region="us-east-1",
        client_location="test",
        results_dir=tmp_path,
        clock=lambda: next(clock),
        revision=lambda: "abc1234",
    )


def test_recorder_writes_header_and_events(tmp_path: Path) -> None:
    with make_recorder(tmp_path) as rec:
        rec.event("sample", label="x", latency_ms=1.5)
    header, events = summarize.load(rec.path)
    assert header["experiment"] == "e0" and header["revision"] == "abc1234"
    assert header["client_location"] == "test"
    assert events == [{**events[0], "kind": "sample", "label": "x", "latency_ms": 1.5}]
    assert rec.path.parent == tmp_path / "e0"


def test_nearest_rank_never_interpolates() -> None:
    values = [float(v) for v in range(1, 101)]
    assert summarize.nearest_rank(values, 50) == 50
    assert summarize.nearest_rank(values, 95) == 95
    assert summarize.nearest_rank(values, 99) == 99
    assert summarize.nearest_rank([3.0, 7.0], 50) == 3.0
    with pytest.raises(ValueError):
        summarize.nearest_rank([], 50)


def test_summarize_file_groups_and_counts_errors(tmp_path: Path) -> None:
    with make_recorder(tmp_path) as rec:
        for v in (10, 20, 30):
            rec.event("sample", label="point", latency_ms=v)
        rec.event("sample", label="point", latency_ms=999, error="RATE_LIMIT_EXCEEDED")
        rec.event("net", host="h", tcp_connect_ms=180)
    _, summaries = summarize.summarize_file(rec.path, "latency_ms")
    (point,) = summaries
    assert (point.n, point.errors, point.p50, point.max) == (3, 1, 20, 30)
    table = summarize.to_markdown(summaries)
    assert "| point | 3 | 1 | 20.0 |" in table


def test_probe_network_reports_three_phases(monkeypatch) -> None:
    class FakeSocket:
        def settimeout(self, _):
            pass

        def connect(self, _):
            pass

        def close(self):
            pass

    class FakeTLS:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(measure.socket, "getaddrinfo", lambda *a, **k: [(2, 1, 6, "", ("127.0.0.1", 443))])
    monkeypatch.setattr(measure.socket, "socket", lambda *a: FakeSocket())
    monkeypatch.setattr(
        measure.ssl,
        "create_default_context",
        lambda: SimpleNamespace(wrap_socket=lambda sock, server_hostname: FakeTLS()),
    )
    timings = measure.probe_network("example.invalid")
    assert set(timings) == {"dns_ms", "tcp_connect_ms", "tls_handshake_ms"}
    assert all(v >= 0 for v in timings.values())


# -- runners -----------------------------------------------------------------


def lightning_payload(rows: list[list]) -> dict:
    return {"status": "success", "rows": {"schema": {}, "data": [{"row": r} for r in rows]}}


class FakeSession:
    """Answers queries from a script of payloads; the last one repeats."""

    def __init__(self, payloads: list) -> None:
        self.payloads = payloads
        self.queries: list[str] = []

    async def query(self, topic, sql, max_result_rows=50):
        self.queries.append(sql)
        payload = self.payloads[min(len(self.queries) - 1, len(self.payloads) - 1)]
        return ToolCall("queryData", {}, False, payload, 12.0)


async def no_sleep(_seconds: float) -> None:
    return None


def test_freshness_records_bounds_around_first_visible_poll(tmp_path: Path) -> None:
    data = datagen.generate(10, 1)
    flight = datagen.probe_flight(data)
    expected = datagen.marker_update(flight, 0).value["estimated_time"].strftime(bench.TIMESTAMP_TEXT)
    stale = lightning_payload([["2026-12-01 00:00:00"]])
    fresh = lightning_payload([[expected]])
    session = FakeSession([stale, stale, fresh])
    rec = make_recorder(tmp_path, ticks=range(0, 10**12, 10**6))  # 1 ms per clock read
    producer = SimpleNamespace(send_one=lambda r: kafka_io.Ack("t", 0, 0, rec.now()))
    asyncio.run(bench.run_freshness(session, producer, rec, flight, samples=1, sleep=no_sleep))
    rec.close()
    _, events = summarize.load(rec.path)
    (sample,) = [e for e in events if e["kind"] == "sample"]
    assert sample["polls"] == 3
    assert 0 < sample["not_seen_before_ms"] < sample["seen_by_ms"]
    assert "B-P000000" in session.queries[0]


def test_freshness_times_out(tmp_path: Path) -> None:
    flight = datagen.probe_flight(datagen.generate(10, 1))
    session = FakeSession([{"status": "error", "error": {"code": "FE_AUTHORIZATION_FAILED"}}])
    rec = make_recorder(tmp_path, ticks=range(0, 10**15, 10**9))  # 1 s per clock read
    producer = SimpleNamespace(send_one=lambda r: kafka_io.Ack("t", 0, 0, rec.now()))
    asyncio.run(bench.run_freshness(session, producer, rec, flight, samples=1, timeout=5, sleep=no_sleep))
    rec.close()
    _, events = summarize.load(rec.path)
    (sample,) = [e for e in events if e["kind"] == "sample"]
    assert sample["error"] == "TIMEOUT"
    assert sample["last_error"] == "FE_AUTHORIZATION_FAILED"


def test_query_latency_runs_every_shape_round_robin(tmp_path: Path) -> None:
    plan = bench.query_shapes(passengers=1000, seed=4, repeat=2)
    assert [s.label for s in plan] == ["point", "filter_at_risk", "range_minutes"] * 2
    assert plan == bench.query_shapes(passengers=1000, seed=4, repeat=2)
    rec = make_recorder(tmp_path)
    asyncio.run(bench.run_query_latency(FakeSession([lightning_payload([["x"]])]), rec, plan))
    rec.close()
    _, summaries = summarize.summarize_file(rec.path, "latency_ms")
    assert {s.label: s.n for s in summaries} == {"point": 2, "filter_at_risk": 2, "range_minutes": 2}


def test_passenger_lookup_rejects_non_generated_ids() -> None:
    assert bench.passenger_lookup("B-P000123").endswith("PASSENGER_ID = 'B-P000123'")
    with pytest.raises(ValueError):
        bench.passenger_lookup("x' OR '1'='1")


def test_concurrency_stops_when_errors_exceed_threshold(tmp_path: Path) -> None:
    error = {"status": "error", "error": {"code": "RATE_LIMIT_EXCEEDED"}}

    @asynccontextmanager
    async def open_session():
        yield FakeSession([lightning_payload([["x"]]), error])

    rec = make_recorder(tmp_path, ticks=range(0, 10**15, 10**8))  # 0.1 s per clock read
    asyncio.run(
        bench.run_concurrency(open_session, rec, levels=[1, 2, 4], duration=1.0, passengers=100, seed=1)
    )
    rec.close()
    _, events = summarize.load(rec.path)
    levels = [e["concurrency"] for e in events if e["kind"] == "level"]
    assert levels == [1]
    assert any(e["kind"] == "stopped" for e in events)


def test_api_key_repr_in_producer_never_leaks() -> None:
    key = ApiKey("K", Secret(FAKE_SECRET))
    assert FAKE_SECRET not in repr(key)
