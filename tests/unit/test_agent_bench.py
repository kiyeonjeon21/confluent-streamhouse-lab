"""E5 (agent on live context) and the source-topic freshness probe, offline."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from streamhouse_lab import bench, datagen, external_agent, kafka_io, summarize
from streamhouse_lab.context import ToolCall
from tests.unit.test_bench import lightning_payload, make_recorder, no_sleep

# -- offers ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("response", "verdict"),
    [
        ("OFFER1=RA214;OFFER2=RA226;HOTEL=NONE", "fresh"),
        (
            "The first hotel is **Harbor Point Hotel**.\n\n" + bench.AGENT_BEFORE,
            "stale",
        ),
        ("OFFER1=RA226;OFFER2=RA232;HOTEL=NONE", "other"),
        ("I could not find any flights.", "missing"),
        (None, "missing"),
    ],
)
def test_classify_offer(response: str | None, verdict: str) -> None:
    assert bench.classify_offer(response) == verdict


def test_offer_line_takes_the_last_offer_and_trims_it() -> None:
    text = "Draft: OFFER1=X;OFFER2=Y;HOTEL=NONE\nOFFER1=RA214;OFFER2=RA226;HOTEL=NONE  \n"
    assert bench.offer_line(text) == bench.AGENT_AFTER


def test_events_change_the_expected_answer() -> None:
    from streamhouse_lab import sqlfiles

    events = sqlfiles.events()
    assert "TIMESTAMP '2026-11-04 16:00:00', 'DELAYED'" in events["ra214-delayed"].statement
    assert "TIMESTAMP '2026-11-04 15:30:00', 'ON_TIME'" in events["ra214-on-time"].statement


# -- the runner -------------------------------------------------------------------


class StatusWorld:
    """Lightning shows RA214's status `lag` polls after each event."""

    def __init__(self, lag: int = 2) -> None:
        self.status, self.pending, self.lag = "ON_TIME", [], lag
        self.events: list[str] = []

    def write(self, status: str) -> None:
        self.events.append(status)
        self.pending = [status] * self.lag

    async def query(self, topic, sql, max_result_rows=50):
        assert topic == bench.CHANGED_FLIGHT_TOPIC and "RA214" in sql
        if self.pending:
            self.pending.pop()
            if not self.pending:
                self.status = self.events[-1]
        return ToolCall("queryData", {}, False, lightning_payload([[self.status]]), 10.0)


def test_agent_runner_records_each_cycle_and_restores_the_flight(tmp_path: Path) -> None:
    world = StatusWorld()
    answers = iter([bench.AGENT_BEFORE, bench.AGENT_AFTER] * 10)
    steps = bench.AgentSteps(
        reset=lambda: world.write("ON_TIME"),
        change=lambda: world.write("DELAYED"),
        agents={
            "flink": lambda: bench.AgentRun([next(answers), bench.AGENT_AFTER], 900.0, run_id="check-e5-1"),
            "claude_code": lambda: bench.AgentRun(["no idea"], 1200.0, tool_calls=3, cost_usd=0.02),
        },
    )
    rec = make_recorder(tmp_path)
    asyncio.run(
        bench.run_agent_freshness(world, steps, rec, offsets=[0, 5], repeats=2, seed=1, sleep=no_sleep)
    )
    rec.close()
    _, events = summarize.load(rec.path)
    samples = [e for e in events if e["kind"] == "sample"]
    assert len(samples) == 4
    assert sorted(s["offset_s"] for s in samples) == [0, 0, 5, 5]
    first = samples[0]
    assert first["lightning"]["seen_by_ms"] > first["lightning"]["not_seen_before_ms"] >= 0
    assert first["agents"]["flink"]["verdicts"] == ["stale", "fresh"]
    assert first["agents"]["flink"]["run_id"] == "check-e5-1"
    assert first["agents"]["claude_code"]["verdicts"] == ["missing"]
    assert first["agents"]["claude_code"]["tool_calls"] == 3
    # Every cycle resets first and changes once; the run ends with RA214 restored.
    assert world.events == ["ON_TIME", "DELAYED"] * 4 + ["ON_TIME"]

    table = summarize.agent_table(rec.path)
    assert "| claude_code | 0 | 0 | 0 | 0 | 2 | 0 | 1.2 | 1.2 | 3-3 |" in table


def test_agent_runner_skips_a_cycle_whose_reset_never_shows(tmp_path: Path) -> None:
    class Stuck(StatusWorld):
        async def query(self, topic, sql, max_result_rows=50):
            return ToolCall("queryData", {}, False, lightning_payload([["DELAYED"]]), 10.0)

    world = Stuck()
    ran = []
    steps = bench.AgentSteps(
        reset=lambda: world.write("ON_TIME"),
        change=lambda: ran.append("change"),
        agents={"flink": lambda: bench.AgentRun([], 1.0)},
    )
    rec = make_recorder(tmp_path, ticks=range(0, 10**15, 10**9))  # 1 s per clock read
    asyncio.run(
        bench.run_agent_freshness(world, steps, rec, offsets=[0], repeats=1, timeout=5, sleep=no_sleep)
    )
    rec.close()
    _, events = summarize.load(rec.path)
    (sample,) = [e for e in events if e["kind"] == "sample"]
    assert sample["error"] == "RESET_TIMEOUT"
    assert ran == []


# -- source-topic freshness ---------------------------------------------------------


def test_source_probe_query_reads_the_flight_directly() -> None:
    assert bench.source_probe_query("B-IN00000") == (
        "SELECT ESTIMATED_TIME FROM bench_flight_status WHERE FLIGHT_ID = 'B-IN00000'"
    )
    with pytest.raises(ValueError, match="not a generated flight id"):
        bench.source_probe_query("RA417' OR '1'='1")


def test_freshness_can_poll_the_source_topic(tmp_path: Path) -> None:
    flight = datagen.probe_flight(datagen.generate(10, 1))
    expected = datagen.marker_update(flight, 0).value["estimated_time"].strftime(bench.TIMESTAMP_TEXT)
    topics = []

    class Session:
        async def query(self, topic, sql, max_result_rows=50):
            topics.append((topic, sql))
            return ToolCall("queryData", {}, False, lightning_payload([[expected]]), 5.0)

    rec = make_recorder(tmp_path)
    producer = SimpleNamespace(send_one=lambda r: kafka_io.Ack("t", 0, 0, rec.now()))
    query = bench.source_probe_query(flight.key["flight_id"])
    asyncio.run(
        bench.run_freshness(
            Session(), producer, rec, flight, samples=1, sleep=no_sleep, topic=bench.SOURCE_TOPIC, query=query
        )
    )
    rec.close()
    assert topics == [(bench.SOURCE_TOPIC, query)]


# -- Claude Code as the external client -------------------------------------------------


def test_agent_prompt_is_read_from_the_sql_and_unescaped() -> None:
    prompt = external_agent.agent_prompt()
    assert prompt.startswith("You are the River Air rebooking agent.")
    assert "passenger's final_destination" in prompt
    assert prompt.rstrip().endswith("OFFER1=<flight_id>;OFFER2=<flight_id>;HOTEL=<hotel or NONE>")


def test_mcp_config_references_the_credential_without_holding_it() -> None:
    config = external_agent.mcp_config("https://mcp.example/mcp")
    server = config["mcpServers"]["rtce"]
    assert server["headers"]["Authorization"] == "${LAB_RTCE_AUTHORIZATION}"
    assert server["url"] == "https://mcp.example/mcp"


def test_command_allows_only_the_two_context_tools(tmp_path: Path) -> None:
    argv = external_agent.command(tmp_path / "mcp.json", "PROMPT")
    assert argv[argv.index("--tools") + 1] == ""
    assert argv[argv.index("--allowedTools") + 1] == "mcp__rtce__queryData,mcp__rtce__getMetadata"
    assert argv[argv.index("--model") + 1] == "claude-sonnet-4-6"
    assert argv[argv.index("--disallowedTools") + 1] == "mcp__rtce__listTopics"
    assert "--strict-mcp-config" in argv
    assert argv[argv.index("-p") + 1] == external_agent.TASK


def test_parse_stream_counts_tool_calls_and_reads_the_result() -> None:
    lines = [
        json.dumps({"type": "system", "subtype": "init"}),
        json.dumps({"type": "assistant", "message": {"content": [{"type": "tool_use", "name": "q"}]}}),
        "not json",
        json.dumps(
            {
                "type": "assistant",
                "message": {"content": [{"type": "text"}, {"type": "tool_use"}, {"type": "tool_use"}]},
            }
        ),
        json.dumps(
            {
                "type": "result",
                "subtype": "success",
                "is_error": False,
                "result": bench.AGENT_AFTER,
                "num_turns": 4,
                "duration_ms": 8123,
                "total_cost_usd": 0.031,
            }
        ),
    ]
    run = external_agent.parse_stream(lines)
    assert run.tool_calls == 3
    assert run.tool_names == ["q", "None", "None"]
    assert run.responses == [bench.AGENT_AFTER]
    assert (run.turns, run.cost_usd, run.error) == (4, 0.031, None)


def test_parse_stream_without_a_result_is_an_error() -> None:
    run = external_agent.parse_stream([json.dumps({"type": "assistant", "message": {"content": []}})])
    assert run.error == "NO_RESULT"
    assert run.responses == []
