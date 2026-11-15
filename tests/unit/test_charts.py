"""Blog figures rendered from recorded Phase 4 results (offline)."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from streamhouse_lab import charts, cli

RUN_HEADER = {"kind": "run", "mono_ns": 0, "experiment": "e2", "params": {}}


def write_results(path: Path, events: list[dict]) -> Path:
    path.write_text("\n".join(json.dumps(e) for e in [RUN_HEADER, *events]) + "\n")
    return path


def stage_event(sample: int, acked_s: float, written: float, committed: float, lightning: float) -> dict:
    return {
        "kind": "sample",
        "label": "stages",
        "sample": sample,
        # The runner logs the sample as soon as Lightning returns the change.
        "mono_ns": int((acked_s + lightning) * 1e9),
        "read_uncommitted_ms": written * 1000,
        "read_committed_ms": committed * 1000,
        "lightning_not_seen_before_ms": (lightning - 1.8) * 1000,
        "lightning_seen_by_ms": lightning * 1000,
    }


def test_histogram_counts_each_value_in_its_bin() -> None:
    assert charts.histogram([0.0, 1.9, 2.0, 69.9], 2.0, 70.0)[:2] == [2, 1]
    assert sum(charts.histogram([0.0, 1.9, 2.0, 69.9], 2.0, 70.0)) == 4
    assert len(charts.histogram([], 2.0, 70.0)) == 35


def test_histogram_rejects_values_outside_the_axis() -> None:
    with pytest.raises(ValueError, match="outside"):
        charts.histogram([70.0], 2.0, 70.0)


def test_axis_upper_is_the_first_tick_above_every_value() -> None:
    assert charts.axis_upper([67.6]) == 70.0
    assert charts.axis_upper([70.0]) == 80.0


def test_freshness_values_skip_timeouts(tmp_path: Path) -> None:
    path = write_results(
        tmp_path / "e1.jsonl",
        [
            {"kind": "sample", "label": "freshness", "seen_by_ms": 17_432.1},
            {"kind": "gap", "seconds": 5.0},
            {"kind": "sample", "label": "freshness", "error": "TIMEOUT"},
        ],
    )
    assert charts.freshness_values(path) == [pytest.approx(17.4321)]


def test_stages_reconstruct_commit_times_from_the_acknowledgement(tmp_path: Path) -> None:
    path = write_results(
        tmp_path / "e2.jsonl",
        [
            stage_event(0, acked_s=100.0, written=2.6, committed=38.5, lightning=44.3),
            {"kind": "sample", "label": "stages", "sample": 1, "error": "TIMEOUT:lightning"},
        ],
    )
    [stage] = charts.stages(path)
    assert (stage.written, stage.committed, stage.lightning_by) == (2.6, 38.5, 44.3)
    assert stage.committed_at == pytest.approx(138.5)


def test_commit_clock_recovers_the_period_across_skipped_intervals() -> None:
    period = 61.2
    ticks = [0, 1, 2, 4, 5, 7]  # intervals 3 and 6 had no sample
    samples = [charts.Stage(i, 1.0, 30.0, 35.0, 500.0 + k * period) for i, k in enumerate(ticks)]
    clock = charts.commit_clock(samples)
    assert clock.period == pytest.approx(period)
    assert clock.max_residual == pytest.approx(0.0, abs=1e-9)
    assert (clock.intervals, clock.samples) == (7, 6)


def test_commit_clock_needs_two_intervals() -> None:
    with pytest.raises(ValueError, match="one interval"):
        charts.commit_clock([charts.Stage(0, 1, 2, 3, 10.0), charts.Stage(1, 1, 2, 3, 12.0)])


@pytest.mark.parametrize("lang", charts.LANGUAGES)
def test_freshness_runs_figure_shares_one_scale(lang: str) -> None:
    fixed = [17.4] + [55.0 + i * 0.1 for i in range(29)]
    jitter = [17.8 + i * 1.7 for i in range(30)]
    figure = charts.render_freshness_runs(fixed, jitter, lang)
    heights = [float(h) for h in re.findall(r"--h: ([\d.]+)%", figure)]
    assert len(heights) == 2 * 35
    assert max(heights) == 100.0
    assert figure.count('class="dist-chart__row"') == 2
    assert "—" not in figure


@pytest.mark.parametrize("lang", charts.LANGUAGES)
def test_stages_figure_has_one_row_per_sample_sorted_by_visibility(lang: str) -> None:
    samples = [
        charts.Stage(0, 2.0, 50.0, 55.0, 0.0),
        charts.Stage(1, 1.0, 12.0, 17.5, 0.0),
        charts.Stage(2, 4.0, 61.0, 68.2, 0.0),
    ]
    figure = charts.render_stages(samples, lang)
    rows = re.findall(r'<div class="stage-chart__row" title="[^"]*">(.*?)</div>', figure)
    assert len(rows) == 3
    totals = [sum(float(w) for w in re.findall(r"--w: ([\d.]+)%", row)) for row in rows]
    assert totals == sorted(totals)
    assert totals[-1] == pytest.approx(68.2 / 70 * 100, abs=0.02)
    assert "--steps: 7" in figure
    assert "—" not in figure


def test_chart_command_checks_the_number_of_files(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    assert cli.main(["chart", "freshness-runs", str(tmp_path / "only-one.jsonl")]) == 2
    assert "needs 2" in capsys.readouterr().err


def test_chart_command_writes_both_languages(tmp_path: Path) -> None:
    path = write_results(
        tmp_path / "e2.jsonl",
        [stage_event(i, 100.0 + i * 70, 2.0, 30.0 + i, 35.0 + i) for i in range(3)],
    )
    out = tmp_path / "charts"
    assert cli.main(["chart", "stages", str(path), "--out", str(out)]) == 0
    assert sorted(p.name for p in out.iterdir()) == ["freshness-stages.en.html", "freshness-stages.ko.html"]
