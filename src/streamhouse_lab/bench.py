"""Phase 4 experiment runners. Methodology: docs/experiments/README.md.

Every runner writes raw events through a `Recorder`; nothing here computes a
headline number. Summaries come from `summarize.py` over the JSON lines, so
the same raw data can be re-analysed later.

Runners take their collaborators as arguments (an MCP session, a producer, a
recorder, a sleep function) so unit tests can drive them with fakes.
"""

from __future__ import annotations

import asyncio
import random
import re
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from streamhouse_lab import datagen
from streamhouse_lab.context import ToolCall, error_code, result_rows
from streamhouse_lab.measure import Recorder

STATE_TOPIC = "bench_passenger_state"
TIMESTAMP_TEXT = "%Y-%m-%d %H:%M:%S"
PASSENGER_ID_RE = re.compile(r"^B-P\d{6}$")

# Fixed query text. Only generated passenger IDs are interpolated, and only after
# validation, so no query is built from untrusted input.
FILTER_AT_RISK = (
    "SELECT PASSENGER_ID, CONNECTION_MINUTES FROM bench_passenger_state WHERE RISK = 'AT_RISK' LIMIT 200"
)
RANGE_MINUTES = (
    "SELECT PASSENGER_ID FROM bench_passenger_state WHERE CONNECTION_MINUTES BETWEEN 45 AND 90 LIMIT 200"
)


def passenger_lookup(passenger_id: str, columns: str = "*") -> str:
    if not PASSENGER_ID_RE.match(passenger_id):
        raise ValueError(f"not a generated passenger id: {passenger_id!r}")
    return f"SELECT {columns} FROM bench_passenger_state WHERE PASSENGER_ID = '{passenger_id}'"  # noqa: S608


class QuerySession(Protocol):
    async def query(self, topic: str, sql: str, max_result_rows: int = 50) -> ToolCall: ...


class Producer(Protocol):
    def send_one(self, record: datagen.Record): ...


Sleep = Callable[[float], Awaitable[None]]


# -- E1: end-to-end freshness ------------------------------------------------


def probe_query(passenger_id: str = datagen.PROBE_PASSENGER) -> str:
    return passenger_lookup(passenger_id, "INBOUND_ARRIVAL")


SOURCE_TOPIC = "bench_flight_status"
FLIGHT_ID_RE = re.compile(r"^B-(IN|OUT)\d{5}$")


def source_probe_query(flight_id: str) -> str:
    """Reads the probe flight straight from the source topic, with no Flink statement in between."""
    if not FLIGHT_ID_RE.match(flight_id):
        raise ValueError(f"not a generated flight id: {flight_id!r}")
    return f"SELECT ESTIMATED_TIME FROM {SOURCE_TOPIC} WHERE FLIGHT_ID = '{flight_id}'"  # noqa: S608


async def run_freshness(
    session: QuerySession,
    producer: Producer,
    recorder: Recorder,
    probe_flight: datagen.Record,
    *,
    samples: int,
    poll_interval: float = 0.25,
    timeout: float = 180.0,
    gap: float = 5.0,
    jitter: float = 60.0,
    seed: int = 0,
    sleep: Sleep = asyncio.sleep,
    topic: str = STATE_TOPIC,
    query: str | None = None,
) -> None:
    """Write a uniquely valued update, then poll Lightning until the derived row shows it.

    By default the poll reads the derived `bench_passenger_state`. With `topic` and
    `query` pointing at the source topic, it measures Lightning's own ingest of a
    plain (non-transactional) Kafka write instead.

    Each sample records two bounds, both relative to the broker acknowledgement:
    `not_seen_before_ms` (start of the last poll that did not see it) and
    `seen_by_ms` (end of the first poll that did). The true visibility time lies
    between them; the gap is the measurement resolution.

    Writes are spaced by `gap` plus a seeded uniform random `jitter`. Without it,
    every write lands right after the previous one became visible, which is just
    after a Flink commit, so every sample measures nearly a full commit interval
    (observed 2026-11-15: 29 of 30 samples at 55 to 60 s with a fixed 5 s gap).
    The jitter should be at least the commit interval so write times are spread
    uniformly across it.
    """
    rng = random.Random(seed)  # noqa: S311 - reproducible spacing, not security
    query = query or probe_query()
    for sample in range(samples):
        update = datagen.marker_update(probe_flight, sample)
        expected = update.value["estimated_time"].strftime(TIMESTAMP_TEXT)
        ack = producer.send_one(update)
        polls = 0
        last_miss_start = ack.acked_ns
        while True:
            started = recorder.now()
            call = await session.query(topic, query, 1)
            finished = recorder.now()
            polls += 1
            rows = result_rows(call.payload)
            seen = bool(rows) and str(rows[0][0]).startswith(expected)
            if seen:
                recorder.event(
                    "sample",
                    label="freshness",
                    sample=sample,
                    expected=expected,
                    not_seen_before_ms=(last_miss_start - ack.acked_ns) / 1e6,
                    seen_by_ms=(finished - ack.acked_ns) / 1e6,
                    polls=polls,
                    last_query_ms=call.elapsed_ms,
                )
                break
            last_miss_start = started
            if (finished - ack.acked_ns) / 1e9 > timeout:
                recorder.event(
                    "sample",
                    label="freshness",
                    sample=sample,
                    expected=expected,
                    error="TIMEOUT",
                    polls=polls,
                    last_error=error_code(call.payload),
                )
                break
            await sleep(poll_interval)
        wait = gap + rng.uniform(0, jitter)
        recorder.event("gap", sample=sample, seconds=wait)
        await sleep(wait)


# -- E3: query latency by shape ----------------------------------------------


@dataclass(frozen=True)
class QueryShape:
    label: str
    sql: str
    max_rows: int


def query_shapes(passengers: int, seed: int, repeat: int) -> list[QueryShape]:
    """Fixed shapes, interleaved round-robin so drift affects every shape equally.

    Point lookups pick passengers with a seeded RNG, so runs are comparable."""
    rng = random.Random(seed)  # noqa: S311 - reproducible sampling, not security
    plan: list[QueryShape] = []
    for _ in range(repeat):
        pid = f"B-P{rng.randrange(passengers):06d}"
        plan += [
            QueryShape("point", passenger_lookup(pid), 1),
            QueryShape("filter_at_risk", FILTER_AT_RISK, 200),
            QueryShape("range_minutes", RANGE_MINUTES, 200),
        ]
    return plan


async def run_query_latency(session: QuerySession, recorder: Recorder, plan: list[QueryShape]) -> None:
    for shape in plan:
        call = await session.query(STATE_TOPIC, shape.sql, shape.max_rows)
        recorder.event(
            "sample",
            label=shape.label,
            latency_ms=call.elapsed_ms,
            rows=len(result_rows(call.payload)),
            error=error_code(call.payload),
        )


# -- E4: throughput under concurrency ----------------------------------------


async def run_concurrency(
    open_session: Callable[[], object],
    recorder: Recorder,
    *,
    levels: list[int],
    duration: float,
    passengers: int,
    seed: int,
    stop_error_rate: float = 0.01,
) -> None:
    """Closed-loop point lookups: at each level, N workers each hold one MCP session and
    issue the next query as soon as the previous one returns, for `duration` seconds.
    Stops before the next level once the error rate exceeds `stop_error_rate`."""
    for level in levels:
        deadline = recorder.now() + int(duration * 1e9)
        counts = {"ok": 0, "error": 0}

        async def worker(worker_id: int, level: int = level, deadline: int = deadline, counts=counts) -> None:
            rng = random.Random(seed * 1000 + worker_id)  # noqa: S311 - reproducible sampling
            async with open_session() as session:
                while recorder.now() < deadline:
                    pid = f"B-P{rng.randrange(passengers):06d}"
                    call = await session.query(STATE_TOPIC, passenger_lookup(pid), 1)
                    code = error_code(call.payload)
                    counts["error" if code else "ok"] += 1
                    recorder.event(
                        "sample",
                        label=f"c{level:03d}",
                        worker=worker_id,
                        latency_ms=call.elapsed_ms,
                        error=code,
                    )

        await asyncio.gather(*(worker(i) for i in range(level)))
        total = counts["ok"] + counts["error"]
        recorder.event(
            "level", concurrency=level, requests=total, errors=counts["error"], duration_s=duration
        )
        if total and counts["error"] / total > stop_error_rate:
            recorder.event("stopped", reason=f"error rate above {stop_error_rate:.0%} at concurrency {level}")
            break


# -- E2: where the freshness goes ----------------------------------------------


class TopicWatcher:
    """Polls a topic reader on a background thread and remembers when each marker first appeared.

    `extract` turns a received record into a marker string, or None to ignore it.
    """

    def __init__(self, reader, extract: Callable[[object], str | None]) -> None:
        self._reader = reader
        self._extract = extract
        self._seen: dict[str, tuple[int, int]] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.error: BaseException | None = None

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                for record in self._reader.poll(0.05):
                    marker = self._extract(record)
                    if marker is not None:
                        with self._lock:
                            self._seen.setdefault(marker, (record.received_ns, record.timestamp_ms))
        except BaseException as exc:  # surfaced to the runner, never swallowed
            self.error = exc

    def start(self) -> TopicWatcher:
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self._reader.close()

    def seen(self, marker: str) -> tuple[int, int] | None:
        """(monotonic ns when first received, the record's broker timestamp in epoch ms)."""
        if self.error is not None:
            raise RuntimeError(f"topic watcher failed: {self.error}") from self.error
        with self._lock:
            return self._seen.get(marker)


def probe_marker(record) -> str | None:
    """The probe passenger's inbound arrival, as the marker text, from a passenger_state record."""
    key, value = record.key, record.value
    if not key or not value or key.get("passenger_id") != datagen.PROBE_PASSENGER:
        return None
    arrival = value.get("inbound_arrival")
    return arrival.strftime(TIMESTAMP_TEXT) if arrival is not None else None


async def run_stage_latency(
    session: QuerySession,
    producer: Producer,
    watchers: dict[str, TopicWatcher],
    recorder: Recorder,
    probe_flight: datagen.Record,
    *,
    samples: int,
    poll_interval: float = 0.25,
    timeout: float = 180.0,
    gap: float = 5.0,
    jitter: float = 60.0,
    seed: int = 0,
    sleep: Sleep = asyncio.sleep,
) -> None:
    """E1 plus Kafka: for each write, when did the output appear to each Kafka reader, and in Lightning?

    With one watcher per isolation level, the gaps decompose freshness:
    read_uncommitted sees Flink's write, read_committed sees it once Flink's
    transaction commits, and Lightning serves it after ingesting committed data.
    """
    rng = random.Random(seed)  # noqa: S311 - reproducible spacing, not security
    query = probe_query()
    for sample in range(samples):
        update = datagen.marker_update(probe_flight, sample)
        marker = update.value["estimated_time"].strftime(TIMESTAMP_TEXT)
        ack = producer.send_one(update)
        polls, last_miss_start, lightning_seen_by = 0, ack.acked_ns, None
        deadline = ack.acked_ns + int(timeout * 1e9)
        while recorder.now() < deadline:
            if lightning_seen_by is None:
                started = recorder.now()
                call = await session.query(STATE_TOPIC, query, 1)
                finished = recorder.now()
                polls += 1
                rows = result_rows(call.payload)
                if rows and str(rows[0][0]).startswith(marker):
                    lightning_seen_by = finished
                else:
                    last_miss_start = started
            if lightning_seen_by is not None and all(w.seen(marker) for w in watchers.values()):
                break
            await sleep(poll_interval)

        fields: dict = {"sample": sample, "marker": marker, "input_ts_ms": ack.timestamp_ms, "polls": polls}
        missing = []
        for label, watcher in watchers.items():
            seen = watcher.seen(marker)
            if seen is None:
                missing.append(label)
                continue
            fields[f"{label}_ms"] = (seen[0] - ack.acked_ns) / 1e6
            fields[f"{label}_record_ts_ms"] = seen[1]
        if lightning_seen_by is None:
            missing.append("lightning")
        else:
            fields["lightning_not_seen_before_ms"] = (last_miss_start - ack.acked_ns) / 1e6
            fields["lightning_seen_by_ms"] = (lightning_seen_by - ack.acked_ns) / 1e6
        if missing:
            fields["error"] = "TIMEOUT:" + ",".join(missing)
        recorder.event("sample", label="stages", **fields)

        wait = gap + rng.uniform(0, jitter)
        recorder.event("gap", sample=sample, seconds=wait)
        await sleep(wait)


# -- E5: an agent grounded in live context -------------------------------------
#
# The agent gets the passenger's landing time in its task and reads flight_status
# and hotel_inventory through Lightning. The ra214-delayed event changes the correct
# answer in all three fields, so each answer shows whether the agent saw the change.

AGENT_BEFORE = "OFFER1=RA226;OFFER2=RA232;HOTEL=Harbor Point Hotel"
AGENT_AFTER = "OFFER1=RA214;OFFER2=RA226;HOTEL=NONE"
OFFER_LINE_RE = re.compile(r"OFFER1=[^;\s]*;OFFER2=[^;\s]*;HOTEL=[^\r\n]*")
CHANGED_FLIGHT_QUERY = "SELECT STATUS FROM flight_status WHERE FLIGHT_ID = 'RA214'"
CHANGED_FLIGHT_TOPIC = "flight_status"


def offer_line(response: str | None) -> str | None:
    """The last OFFER line in a response; models sometimes add a sentence before it."""
    found = OFFER_LINE_RE.findall(response or "")
    return found[-1].strip() if found else None


def classify_offer(response: str | None) -> str:
    """fresh: answered from the changed data; stale: from the data before the change;
    other: some other offer; missing: no offer line at all."""
    line = offer_line(response)
    if line is None:
        return "missing"
    if line == AGENT_AFTER:
        return "fresh"
    if line == AGENT_BEFORE:
        return "stale"
    return "other"


@dataclass
class AgentRun:
    """One invocation of an agent; `responses` has one entry per passenger it answered for."""

    responses: list[str]
    elapsed_ms: float
    run_id: str | None = None  # the Flink statement name, which keys the agent's system log
    tool_calls: int | None = None
    turns: int | None = None
    cost_usd: float | None = None
    error: str | None = None


@dataclass
class AgentSteps:
    reset: Callable[[], None]
    change: Callable[[], None]
    agents: dict[str, Callable[[], AgentRun]]


async def poll_status(
    session: QuerySession,
    recorder: Recorder,
    wanted: str,
    reference_ns: int,
    *,
    poll_interval: float,
    timeout: float,
    sleep: Sleep,
) -> dict:
    """Poll RA214's status in Lightning until it reads `wanted`; bounds are relative to `reference_ns`."""
    polls, last_miss_start = 0, reference_ns
    while True:
        started = recorder.now()
        call = await session.query(CHANGED_FLIGHT_TOPIC, CHANGED_FLIGHT_QUERY, 1)
        finished = recorder.now()
        polls += 1
        rows = result_rows(call.payload)
        if rows and rows[0][0] == wanted:
            return {
                "not_seen_before_ms": (last_miss_start - reference_ns) / 1e6,
                "seen_by_ms": (finished - reference_ns) / 1e6,
                "polls": polls,
            }
        last_miss_start = started
        if (finished - reference_ns) / 1e9 > timeout:
            return {"error": "TIMEOUT", "polls": polls, "last_error": error_code(call.payload)}
        await sleep(poll_interval)


def describe_run(run: AgentRun) -> dict:
    return {
        "elapsed_ms": run.elapsed_ms,
        "run_id": run.run_id,
        "verdicts": [classify_offer(r) for r in run.responses],
        "single_line": [len((r or "").strip().splitlines()) == 1 for r in run.responses],
        "offers": [offer_line(r) for r in run.responses],
        "responses": [(r or "")[:600] for r in run.responses],
        "tool_calls": run.tool_calls,
        "turns": run.turns,
        "cost_usd": run.cost_usd,
        "error": run.error,
    }


async def run_agent_freshness(
    session: QuerySession,
    steps: AgentSteps,
    recorder: Recorder,
    *,
    offsets: list[float],
    repeats: int,
    seed: int = 0,
    poll_interval: float = 0.25,
    timeout: float = 120.0,
    sleep: Sleep = asyncio.sleep,
) -> None:
    """For each cycle: restore RA214, wait until Lightning shows it on time, write the
    delay, wait `offset` seconds, then run every agent at once on the same question.

    Offsets are shuffled within each repeat with a seeded RNG, so drift over the run
    does not line up with one offset. Lightning's own visibility of the delay is
    polled in the same cycle, so each answer can be read against what the serving
    layer showed at the time.
    """
    rng = random.Random(seed)  # noqa: S311 - reproducible order, not security
    plan: list[float] = []
    for _ in range(repeats):
        order = list(offsets)
        rng.shuffle(order)
        plan += order
    for cycle, offset in enumerate(plan):
        await asyncio.to_thread(steps.reset)
        polling = {"poll_interval": poll_interval, "timeout": timeout, "sleep": sleep}
        reset = await poll_status(session, recorder, "ON_TIME", recorder.now(), **polling)
        if "error" in reset:
            recorder.event(
                "sample", label="agent", cycle=cycle, offset_s=offset, error="RESET_" + reset["error"]
            )
            continue
        await asyncio.to_thread(steps.change)
        written = recorder.now()
        visibility = asyncio.create_task(poll_status(session, recorder, "DELAYED", written, **polling))
        await sleep(offset)
        started = recorder.now()
        names = list(steps.agents)
        runs = await asyncio.gather(*(asyncio.to_thread(steps.agents[n]) for n in names))
        seen = await visibility
        recorder.event(
            "sample",
            label="agent",
            cycle=cycle,
            offset_s=offset,
            started_after_ms=(started - written) / 1e6,
            reset_seen_by_ms=reset.get("seen_by_ms"),
            lightning=seen,
            agents={n: describe_run(r) for n, r in zip(names, runs, strict=True)},
        )
    # Leave the quick start data as it was: RA214 on time.
    await asyncio.to_thread(steps.reset)


# -- Hybrid read: Iceberg history plus the Kafka tail ---------------------------

HYBRID_TABLE = "hybrid_flight_history"
OLD_BATCH = frozenset({"OLD-1", "OLD-2", "OLD-3", "OLD-4"})
NEW_BATCH = frozenset({"NEW-1", "NEW-2"})


@dataclass
class HybridSteps:
    """The outside world the hybrid experiment touches, injected so it can run against fakes."""

    write_old: Callable[[], None]
    write_new: Callable[[], None]
    kafka_ids: Callable[[], list[str]]
    iceberg_ids: Callable[[], list[str]]
    flink_ids: Callable[[], list[str]]
    truncate: Callable[[], dict]


def wait_for(
    probe: Callable[[], bool], *, timeout: float, interval: float, clock, sleep_sync
) -> float | None:
    start = clock()
    while True:
        if probe():
            return clock() - start
        if clock() - start > timeout:
            return None
        sleep_sync(interval)


def run_hybrid(
    steps: HybridSteps,
    recorder: Recorder,
    *,
    iceberg_timeout: float = 1200.0,
    flink_timeout: float = 600.0,
    interval: float = 30.0,
    clock: Callable[[], float] = time.monotonic,
    sleep_sync: Callable[[float], None] = time.sleep,
    resume: bool = False,
) -> dict:
    """Prove that a Flink snapshot query reads Iceberg for data that Kafka no longer has.

    With `resume`, steps 1 and 2 are taken as already done (for example by a run that
    stopped after truncating); the run starts by observing that state instead.

    1. Write an old batch; wait until Tableflow has it in Iceberg.
    2. Delete every record from the Kafka topic (DeleteRecords), as retention eventually would.
    3. A snapshot query that still returns the old batch can only have read it from Iceberg.
    4. Write a new batch; the same query must now return both: Iceberg history plus the Kafka tail.
    """

    def observe(step: str, **extra) -> dict:
        state = {
            "kafka": sorted(set(steps.kafka_ids())),
            "iceberg": sorted(set(steps.iceberg_ids())),
            "flink": sorted(set(steps.flink_ids())),
        }
        recorder.event("step", step=step, **state, **extra)
        return state

    if resume:
        after_truncate = observe("kafka_truncated", resumed=True)
    else:
        steps.write_old()
        waited = wait_for(
            lambda: OLD_BATCH <= set(steps.iceberg_ids()),
            timeout=iceberg_timeout, interval=interval, clock=clock, sleep_sync=sleep_sync,
        )  # fmt: skip
        before = observe("old_batch_in_iceberg", waited_s=waited)
        if waited is None:
            return {"verdict": "inconclusive", "reason": "old batch never reached Iceberg", "before": before}
        start_offsets = steps.truncate()
        after_truncate = observe("kafka_truncated", start_offsets=start_offsets)

    steps.write_new()
    waited_new = wait_for(
        lambda: NEW_BATCH <= set(steps.flink_ids()),
        timeout=flink_timeout, interval=interval, clock=clock, sleep_sync=sleep_sync,
    )  # fmt: skip
    after_new = observe("new_batch_written", waited_s=waited_new)

    old_only_in_iceberg = not (OLD_BATCH & set(after_truncate["kafka"])) and OLD_BATCH <= set(
        after_truncate["iceberg"]
    )
    flink_read_iceberg = OLD_BATCH <= set(after_truncate["flink"])
    flink_read_both = (OLD_BATCH | NEW_BATCH) <= set(after_new["flink"])
    verdict = "hybrid" if old_only_in_iceberg and flink_read_iceberg and flink_read_both else "not shown"
    result = {
        "verdict": verdict,
        "old_only_in_iceberg": old_only_in_iceberg,
        "flink_read_iceberg_after_truncate": flink_read_iceberg,
        "flink_read_iceberg_and_kafka": flink_read_both,
    }
    recorder.event("verdict", **result)
    return result
