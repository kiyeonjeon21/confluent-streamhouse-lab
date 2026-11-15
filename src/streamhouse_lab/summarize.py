"""Turns experiment JSON lines into percentile tables.

Percentiles use the nearest-rank method: the p-th percentile of n sorted
samples is the value at rank ceil(p/100 * n). It never interpolates, so every
reported number is a latency that was actually observed.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

PERCENTILES = (50, 95, 99)


def nearest_rank(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        raise ValueError("no samples")
    if not 0 < p <= 100:
        raise ValueError("p must be in (0, 100]")
    rank = math.ceil(p / 100 * len(sorted_values))
    return sorted_values[rank - 1]


@dataclass
class Summary:
    label: str
    n: int
    errors: int
    p50: float | None
    p95: float | None
    p99: float | None
    max: float | None


def summarize(values: Iterable[float], label: str, errors: int = 0) -> Summary:
    ordered = sorted(values)
    if not ordered:
        return Summary(label, 0, errors, None, None, None, None)
    p50, p95, p99 = (nearest_rank(ordered, p) for p in PERCENTILES)
    return Summary(label, len(ordered), errors, p50, p95, p99, ordered[-1])


def load(path: Path) -> tuple[dict, list[dict]]:
    lines = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not lines or lines[0].get("kind") != "run":
        raise ValueError(f"{path} does not start with a run header")
    return lines[0], lines[1:]


def summarize_file(path: Path, metric: str, group_by: str = "label") -> tuple[dict, list[Summary]]:
    """Summarize `metric` over `sample` events, grouped by a field (errors counted separately)."""
    header, events = load(path)
    groups: dict[str, list[float]] = defaultdict(list)
    errors: dict[str, int] = defaultdict(int)
    for event in events:
        if event.get("kind") != "sample":
            continue
        key = str(event.get(group_by, "all"))
        if event.get("error"):
            errors[key] += 1
        elif event.get(metric) is not None:
            groups[key].append(float(event[metric]))
    labels = sorted(set(groups) | set(errors))
    return header, [summarize(groups[k], k, errors[k]) for k in labels]


def to_markdown(summaries: list[Summary], unit: str = "ms") -> str:
    def fmt(value: float | None) -> str:
        return "-" if value is None else f"{value:.1f}"

    lines = [
        f"| label | n | errors | p50 ({unit}) | p95 ({unit}) | p99 ({unit}) | max ({unit}) |",
        "|---|---|---|---|---|---|---|",
    ]
    for s in summaries:
        cells = [s.label, str(s.n), str(s.errors), fmt(s.p50), fmt(s.p95), fmt(s.p99), fmt(s.max)]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)
