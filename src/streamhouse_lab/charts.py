"""Renders Phase 4 results as static HTML figures for the blog (offline).

The figures are plain HTML styled by `pages/assets/main.scss`, so their text
stays readable at phone widths. They are generated from the raw JSON-lines
files under `results/` (gitignored) and committed under
`pages/_includes/charts/`, one file per figure and language.

Nothing here talks to Confluent Cloud.
"""

from __future__ import annotations

import html
import math
from dataclasses import dataclass
from pathlib import Path

from streamhouse_lab import summarize

LANGUAGES = ("en", "ko")
BIN_SECONDS = 2.0
AXIS_STEP_SECONDS = 10.0

TEXT = {
    "en": {
        "seconds": "{value} s",
        "runs_title": "Fixed gaps lined the samples up with Flink's commits",
        "runs_note": (
            "Seconds from the broker acknowledgement until Lightning served the change, "
            "in 2-second bins, 30 samples per run."
        ),
        "fixed_label": "Fixed 5 s gap",
        "jitter_label": "Random 5 to 65 s gap",
        "fixed_stats": "First run, p50 {p50}",
        "jitter_stats": "Second run, p50 {p50}",
        "bin_title": "{low} to {high} s: {count}",
        "count": ["{n} sample", "{n} samples"],
        "run_summary": "{label}: {n} samples from {low} to {high}, median {p50}.",
        "runs_summary": "Histogram of freshness for two runs.",
        "stages_title": "Every sample spent most of its time waiting for the commit",
        "stages_note": (
            "One row per sample, sorted by when Lightning served the change. "
            "Seconds after the broker acknowledged the source write."
        ),
        "write": "Flink wrote output",
        "commit": "Waiting for commit",
        "lightning": "Commit to Lightning",
        "row_title": "Sample {sample}: written {write}, committed {commit}, in Lightning by {lightning}",
        "stages_summary": (
            "Stacked bars for {n} samples. Flink wrote the output within {write_max}, "
            "the commit came between {commit_min} and {commit_max}, "
            "and the first Lightning query to return it finished {lightning_min} to {lightning_max} "
            "after the commit."
        ),
    },
    "ko": {
        "seconds": "{value}초",
        "runs_title": "고정 간격이 샘플을 Flink commit에 맞춰 버렸다",
        "runs_note": (
            "Broker 승인부터 Lightning이 변경을 반환할 때까지의 시간이며, 2초 구간으로 묶었다. "
            "실행당 샘플 30개."
        ),
        "fixed_label": "고정 5초 간격",
        "jitter_label": "무작위 5\u201365초 간격",
        "fixed_stats": "첫 실행, p50 {p50}",
        "jitter_stats": "두 번째 실행, p50 {p50}",
        "bin_title": "{low}\u2013{high}초: {count}",
        "count": ["샘플 {n}개", "샘플 {n}개"],
        "run_summary": "{label}: 샘플 {n}개, {low}부터 {high}까지, 중앙값 {p50}.",
        "runs_summary": "두 실행의 freshness 히스토그램.",
        "stages_title": "모든 샘플이 대부분의 시간을 commit 대기에 썼다",
        "stages_note": (
            "샘플 하나가 한 줄이며 Lightning이 변경을 반환한 순서로 정렬했다. "
            "Source write의 broker 승인 이후 초 단위."
        ),
        "write": "Flink output write",
        "commit": "commit 대기",
        "lightning": "commit에서 Lightning까지",
        "row_title": "샘플 {sample}: write {write}, commit {commit}, Lightning {lightning}까지",
        "stages_summary": (
            "샘플 {n}개의 누적 막대. Flink는 {write_max} 안에 output을 썼고, "
            "commit은 {commit_min}에서 {commit_max} 사이에 이뤄졌으며, "
            "변경을 처음 반환한 Lightning query는 "
            "commit 뒤 {lightning_min}에서 {lightning_max} 사이에 끝났다."
        ),
    },
}


@dataclass(frozen=True)
class Stage:
    """One E2 sample, in seconds after the broker acknowledged the source write."""

    sample: int
    written: float
    committed: float
    lightning_by: float
    committed_at: float  # client monotonic seconds when the read_committed consumer saw the output


@dataclass(frozen=True)
class CommitClock:
    period: float
    max_residual: float
    intervals: int
    samples: int


def freshness_values(path: Path) -> list[float]:
    """E1 "visible by" times in seconds, skipping samples that timed out."""
    _, events = summarize.load(path)
    return [
        event["seen_by_ms"] / 1000
        for event in events
        if event.get("kind") == "sample" and not event.get("error") and event.get("seen_by_ms") is not None
    ]


def stages(path: Path) -> list[Stage]:
    """E2 samples with every stage observed, in recorded order."""
    _, events = summarize.load(path)
    result = []
    for event in events:
        if event.get("kind") != "sample" or event.get("error"):
            continue
        # The runner logs the sample right after Lightning returns the change,
        # so the event's monotonic time minus that offset is the acknowledgement.
        acked = event["mono_ns"] / 1e9 - event["lightning_seen_by_ms"] / 1000
        result.append(
            Stage(
                sample=event["sample"],
                written=event["read_uncommitted_ms"] / 1000,
                committed=event["read_committed_ms"] / 1000,
                lightning_by=event["lightning_seen_by_ms"] / 1000,
                committed_at=acked + event["read_committed_ms"] / 1000,
            )
        )
    return result


def commit_clock(samples: list[Stage], nominal: float = 61.0) -> CommitClock:
    """Fits evenly spaced commits to the observed commit times.

    Each commit is assigned to the nearest whole number of nominal intervals
    after the first, then a least-squares line gives the period. The largest
    residual says how well a fixed clock explains every observation.
    """
    if len(samples) < 2:
        raise ValueError("need at least two samples")
    times = [s.committed_at for s in samples]
    ticks = [round((t - times[0]) / nominal) for t in times]
    if len(set(ticks)) < 2:
        raise ValueError("all commits fall in one interval")
    mean_tick = sum(ticks) / len(ticks)
    mean_time = sum(times) / len(times)
    period = sum((k - mean_tick) * (t - mean_time) for k, t in zip(ticks, times, strict=True)) / sum(
        (k - mean_tick) ** 2 for k in ticks
    )
    intercept = mean_time - period * mean_tick
    residual = max(abs(t - (intercept + period * k)) for k, t in zip(ticks, times, strict=True))
    return CommitClock(period, residual, ticks[-1] - ticks[0], len(samples))


def histogram(values: list[float], bin_width: float, upper: float) -> list[int]:
    bins = [0] * math.ceil(upper / bin_width)
    for value in values:
        if not 0 <= value < upper:
            raise ValueError(f"{value} is outside 0 to {upper}")
        bins[int(value // bin_width)] += 1
    return bins


def axis_upper(values: list[float], step: float = AXIS_STEP_SECONDS) -> float:
    """The first tick strictly above every value, so each value has a histogram bin."""
    return (math.floor(max(values) / step) + 1) * step


def percent(value: float, upper: float) -> str:
    return f"{value / upper * 100:.2f}%"


def number(value: float) -> str:
    return f"{value:.1f}" if value % 1 else f"{value:.0f}"


def seconds(lang: str, value: float) -> str:
    return TEXT[lang]["seconds"].format(value=number(value))


def count_text(lang: str, n: int) -> str:
    singular, plural = TEXT[lang]["count"]
    return (singular if n == 1 else plural).format(n=n)


def axis(lang: str, upper: float, modifier: str = "") -> str:
    ticks = []
    steps = round(upper / AXIS_STEP_SECONDS)
    for i in range(steps + 1):
        value = i * AXIS_STEP_SECONDS
        label = seconds(lang, value) if i == steps else number(value)
        ticks.append(f'<span style="--x: {percent(value, upper)}">{html.escape(label)}</span>')
    css = "chart-axis" + (f" {modifier}" if modifier else "")
    return f'<div class="{css}" aria-hidden="true">{"".join(ticks)}</div>'


def render_freshness_runs(fixed: list[float], jitter: list[float], lang: str) -> str:
    t = TEXT[lang]
    upper = axis_upper(fixed + jitter)
    runs = [
        (t["fixed_label"], t["fixed_stats"], fixed, "dist-chart__plot--muted"),
        (t["jitter_label"], t["jitter_stats"], jitter, ""),
    ]
    tallest = max(max(histogram(values, BIN_SECONDS, upper)) for _, _, values, _ in runs)
    figure_id = f"freshness-runs-{lang}"
    rows, summaries = [], [t["runs_summary"]]
    for label, stats, values, modifier in runs:
        ordered = sorted(values)
        p50 = seconds(lang, round(summarize.nearest_rank(ordered, 50), 1))
        summaries.append(
            t["run_summary"].format(
                label=label,
                n=len(values),
                low=seconds(lang, round(ordered[0], 1)),
                high=seconds(lang, round(ordered[-1], 1)),
                p50=p50,
            )
        )
        bars = []
        for i, count in enumerate(histogram(values, BIN_SECONDS, upper)):
            low, high = i * BIN_SECONDS, (i + 1) * BIN_SECONDS
            title = t["bin_title"].format(low=number(low), high=number(high), count=count_text(lang, count))
            height = f"{count / tallest * 100:.2f}%"
            bars.append(f'<i style="--h: {height}" title="{html.escape(title)}"></i>')
        plot_css = "dist-chart__plot" + (f" {modifier}" if modifier else "")
        rows.append(
            '<div class="dist-chart__row">'
            f'<p class="dist-chart__label"><strong>{html.escape(label)}</strong>'
            f"<span>{html.escape(stats.format(p50=p50))}</span></p>"
            f'<div class="{plot_css}">{"".join(bars)}</div>'
            "</div>"
        )
    return (
        f'<figure class="benchmark-chart dist-chart" aria-labelledby="{figure_id}-title">\n'
        f'  <figcaption id="{figure_id}-title"><strong>{html.escape(t["runs_title"])}</strong>'
        f"<span>{html.escape(t['runs_note'])}</span></figcaption>\n"
        f'  <div class="dist-chart__body" role="img" aria-label="{html.escape(" ".join(summaries))}">\n'
        + "".join(f"    {row}\n" for row in rows)
        + f"    {axis(lang, upper, 'dist-chart__axis')}\n"
        "  </div>\n"
        "</figure>\n"
    )


def render_stages(samples: list[Stage], lang: str) -> str:
    t = TEXT[lang]
    upper = axis_upper([s.lightning_by for s in samples])
    figure_id = f"freshness-stages-{lang}"
    rows = []
    for s in sorted(samples, key=lambda s: s.lightning_by):
        title = t["row_title"].format(
            sample=s.sample + 1,
            write=seconds(lang, round(s.written, 1)),
            commit=seconds(lang, round(s.committed, 1)),
            lightning=seconds(lang, round(s.lightning_by, 1)),
        )
        segments = [
            ("write", s.written),
            ("commit", s.committed - s.written),
            ("lightning", max(0.0, s.lightning_by - s.committed)),
        ]
        bars = "".join(
            f'<i class="seg--{name}" style="--w: {percent(width, upper)}"></i>' for name, width in segments
        )
        rows.append(f'<div class="stage-chart__row" title="{html.escape(title)}">{bars}</div>')
    legend = "".join(
        f'<span><i class="seg--{name}"></i>{html.escape(t[name])}</span>'
        for name in ("write", "commit", "lightning")
    )
    summary = t["stages_summary"].format(
        n=len(samples),
        write_max=seconds(lang, round(max(s.written for s in samples), 1)),
        commit_min=seconds(lang, round(min(s.committed for s in samples), 1)),
        commit_max=seconds(lang, round(max(s.committed for s in samples), 1)),
        lightning_min=seconds(lang, round(min(s.lightning_by - s.committed for s in samples), 1)),
        lightning_max=seconds(lang, round(max(s.lightning_by - s.committed for s in samples), 1)),
    )
    return (
        f'<figure class="benchmark-chart stage-chart" aria-labelledby="{figure_id}-title">\n'
        f'  <figcaption id="{figure_id}-title"><strong>{html.escape(t["stages_title"])}</strong>'
        f"<span>{html.escape(t['stages_note'])}</span></figcaption>\n"
        f'  <div class="benchmark-chart__legend stage-chart__legend" aria-hidden="true">{legend}</div>\n'
        f'  <div class="stage-chart__plot" style="--steps: {round(upper / AXIS_STEP_SECONDS)}" '
        f'role="img" aria-label="{html.escape(summary)}">\n'
        + "".join(f"    {row}\n" for row in rows)
        + "  </div>\n"
        f"  {axis(lang, upper)}\n"
        "</figure>\n"
    )


def write_figures(name: str, render, out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for lang in LANGUAGES:
        path = out_dir / f"{name}.{lang}.html"
        path.write_text(render(lang))
        written.append(path)
    return written
