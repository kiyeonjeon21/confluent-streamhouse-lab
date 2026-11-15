"""E6 scenarios against a fake pipeline (offline)."""

from __future__ import annotations

from pathlib import Path

import pytest

from streamhouse_lab import bench, failure, summarize
from tests.unit.test_bench import make_recorder


class FakePipeline:
    """A materialized table that applies probe writes only while running, after `lag` seconds."""

    def __init__(self, lag: float = 3.0) -> None:
        self.now = 0.0
        self.lag = lag
        self.phase, self.version, self.stopped = "RUNNING", 1, False
        self.state, self.source = "2026-12-01 08:00:00", "2026-12-01 08:00:00"
        self.pending: list[tuple[float, str]] = []
        self.sql: list[str] = []
        self.unreadable = 0

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds
        if not self.stopped:
            for at, value in list(self.pending):
                if at <= self.now:
                    self.state = value
                    self.pending.remove((at, value))

    def set_stopped(self, stopped: bool) -> None:
        self.stopped = stopped
        self.phase = "STOPPED" if stopped else "RUNNING"
        if not stopped:  # resume: queued writes apply after the lag
            self.pending = [(self.now + self.lag, v) for _, v in self.pending]

    def write_marker(self, i: int) -> str:
        value = f"2026-12-01 09:{i % 60:02d}:00"
        self.source = value
        self.pending.append((self.now + self.lag, value))
        return value

    def world(self) -> failure.World:
        return failure.World(
            mt_status=lambda: {"phase": self.phase, "version": self.version, "stopped": self.stopped},
            set_stopped=self.set_stopped,
            probe_state=lambda: self.state,
            probe_source=lambda: self.source,
            write_marker=self.write_marker,
            counts=lambda: {"OK": 6, "AT_RISK": 2, "NO_CONNECTION": 2},
            lightning_sample=lambda risk: 5,
            run_sql=self.run_sql,
            write_new_passenger=lambda: "B-P999990",
            passenger_visible=lambda pid: True,
            write_unreadable=lambda: setattr(self, "unreadable", self.unreadable + 1),
            topic_status=lambda: {"lightning:bench_flight_status": {"status": "ACTIVE"}},
            clock=self.clock,
            sleep=self.sleep,
        )

    def run_sql(self, sql: str) -> None:
        self.sql.append(sql)
        self.version += 1


def steps(path: Path) -> list[str]:
    _, events = summarize.load(path)
    return [e["step"] for e in events if e["kind"] == "step"]


def test_stop_resume_shows_stale_reads_then_times_the_catch_up(tmp_path: Path) -> None:
    pipeline = FakePipeline(lag=3.0)
    rec = make_recorder(tmp_path)
    result = failure.stop_resume(pipeline.world(), rec, writes=2, gap=5, hold=10)
    rec.close()
    assert result["stop_s"] == 0
    assert result["lightning_stale_while_stopped"] is True
    assert result["caught_up_s"] == pytest.approx(3.0)
    assert pipeline.state == "2026-12-01 09:41:00"  # the last write, marker 101
    assert steps(rec.path) == ["before", "stopped", "write_0_while_stopped", "write_1_while_stopped",
                               "held_stopped", "resumed"]  # fmt: skip


def test_evolve_records_zombies_and_whether_old_and_new_keys_update(tmp_path: Path) -> None:
    pipeline = FakePipeline(lag=2.0)
    rec = make_recorder(tmp_path)
    result = failure.evolve(pipeline.world(), rec, "CREATE OR ALTER ...", settle=1)
    rec.close()
    assert pipeline.sql == ["CREATE OR ALTER ..."]
    assert result["no_connection_before"] == result["no_connection_after"] == 2
    assert result["lightning_no_connection_after"] == 5
    assert result["existing_passenger_updated_s"] is not None
    assert result["new_passenger_visible_s"] == 0


def test_unreadable_record_writes_once_and_a_valid_write_a_minute_later(tmp_path: Path) -> None:
    pipeline = FakePipeline(lag=2.0)
    rec = make_recorder(tmp_path)
    result = failure.unreadable_record(pipeline.world(), rec, watch=120, interval=30)
    rec.close()
    assert pipeline.unreadable == 1
    assert result["valid_write_in_lightning_source"] is True
    assert result["valid_write_in_lightning_state"] is True
    assert steps(rec.path).count("after_unreadable") == 4


def test_wait_until_gives_up_after_the_timeout() -> None:
    pipeline = FakePipeline()
    assert failure.wait_until(pipeline.world(), lambda: False, timeout=5, interval=2) is None
    assert pipeline.now == 6


def test_risk_sample_query_accepts_only_known_classes() -> None:
    assert "RISK = 'NO_CONNECTION' LIMIT 5" in bench.risk_sample_query("NO_CONNECTION")
    with pytest.raises(ValueError, match="unknown risk"):
        bench.risk_sample_query("OK' OR '1'='1")
