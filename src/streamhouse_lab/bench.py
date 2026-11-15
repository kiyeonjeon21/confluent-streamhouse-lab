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
    sleep: Sleep = asyncio.sleep,
) -> None:
    """Write a uniquely valued update, then poll Lightning until the derived row shows it.

    Each sample records two bounds, both relative to the broker acknowledgement:
    `not_seen_before_ms` (start of the last poll that did not see it) and
    `seen_by_ms` (end of the first poll that did). The true visibility time lies
    between them; the gap is the measurement resolution.
    """
    query = probe_query()
    for sample in range(samples):
        update = datagen.marker_update(probe_flight, sample)
        expected = update.value["estimated_time"].strftime(TIMESTAMP_TEXT)
        ack = producer.send_one(update)
        polls = 0
        last_miss_start = ack.acked_ns
        while True:
            started = recorder.now()
            call = await session.query(STATE_TOPIC, query, 1)
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
        await sleep(gap)


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
