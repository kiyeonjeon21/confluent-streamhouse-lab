"""Applies SQL stages idempotently and runs their checks."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

from streamhouse_lab import sqlfiles
from streamhouse_lab.flink import FlinkClient, QueryResult, Statement

# Bounded statements end COMPLETED. CREATE MATERIALIZED TABLE was seen RUNNING right after
# submission and COMPLETED later (2026-11-15); its continuous query is tracked separately
# under /materialized-tables, not in the statement list.
APPLIED = frozenset({"COMPLETED", "RUNNING"})


@dataclass
class Applied:
    name: str
    phase: str
    skipped: bool


def apply_stage(
    client: FlinkClient, stage: sqlfiles.Stage, log: Callable[[str], None] = print
) -> list[Applied]:
    """Submit each statement in order. A statement that already exists and is healthy is skipped,
    so re-running a stage never duplicates seed events."""
    applied: list[Applied] = []
    for sql_file in stage.statements():
        name = stage.statement_name(sql_file)
        existing = client.get(name)
        if existing is not None and existing.phase in APPLIED:
            log(f"skip  {name} ({existing.phase})")
            applied.append(Applied(name, existing.phase, skipped=True))
            continue
        if existing is not None:
            log(f"retry {name} (was {existing.phase}: {existing.detail})")
            client.delete(name)
        log(f"apply {name} [{sql_file.mode}]")
        client.submit(name, sql_file.statement, mode=sql_file.mode)
        statement = client.wait(name, until=APPLIED)
        applied.append(Applied(name, statement.phase, skipped=False))
    return applied


def run_checks(client: FlinkClient, stage: sqlfiles.Stage) -> dict[str, QueryResult]:
    results = {}
    for sql_file in stage.checks():
        name = sqlfiles.statement_name("check", stage.name, sql_file.stem, str(int(time.time())))
        results[sql_file.stem] = client.query(name, sql_file.statement)
    return results


def run_event(client: FlinkClient, event: str) -> Statement:
    """Events are replayable, so each run gets a fresh, timestamped statement name."""
    sql_file = sqlfiles.events()[event]
    name = sqlfiles.statement_name("event", event, str(int(time.time() * 1000)))
    client.submit(name, sql_file.statement, mode=sql_file.mode)
    return client.wait(name, until=frozenset({"COMPLETED"}))
