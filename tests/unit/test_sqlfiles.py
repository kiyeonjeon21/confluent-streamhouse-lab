"""Static checks on every SQL file, so a broken file fails here and not halfway through a paid deploy."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from streamhouse_lab import sqlfiles
from streamhouse_lab.config import SQL_DIR

ALL_SQL = sorted(SQL_DIR.rglob("*.sql"))


def test_expected_stages_exist() -> None:
    assert [s.name for s in sqlfiles.stages()] == [
        "sources",
        "transform",
        "passenger-state",
        "history",
        "agent",
        "bench",
        "hybrid",
    ]


@pytest.mark.parametrize("path", ALL_SQL, ids=lambda p: str(p.relative_to(SQL_DIR)))
def test_each_file_parses_to_one_statement(path: Path) -> None:
    sql_file = sqlfiles.parse(path)
    assert sql_file.statement
    assert not sql_file.statement.rstrip().endswith(";")


@pytest.mark.parametrize("path", ALL_SQL, ids=lambda p: str(p.relative_to(SQL_DIR)))
def test_no_environment_specific_ids_or_secrets(path: Path) -> None:
    text = path.read_text()
    # Catalog and database come from statement properties; IDs come from Terraform.
    assert not re.search(r"\b(env|lkc|lfcp|sa|u|lsrc)-[a-z0-9]{4,}\b", text)
    assert not re.search(r"\b[A-Z0-9]{16}\b", text), "looks like an API key ID"
    assert "river-air-lab" not in text


def test_checks_are_snapshot_queries() -> None:
    for stage in sqlfiles.stages():
        for check in stage.checks():
            assert check.mode == "snapshot", check.path
            assert check.statement.lstrip().upper().startswith("SELECT"), check.path


def test_statement_names_are_valid_and_unique() -> None:
    names = [s.statement_name(f) for s in sqlfiles.stages() for f in s.statements()]
    assert len(names) == len(set(names))
    for name in names:
        assert sqlfiles.STATEMENT_NAME_RE.match(name)
        assert len(name) <= sqlfiles.STATEMENT_NAME_MAX


def test_events_exist() -> None:
    assert {"ra417-delayed", "ra417-on-time"} <= set(sqlfiles.events())


def test_doubled_quotes_and_comments_do_not_split() -> None:
    text = "-- header; with semicolon\nSELECT 'it''s; fine', `a;b` /* c; */ FROM t;"
    assert sqlfiles.split_statements(text) == ["SELECT 'it''s; fine', `a;b`  FROM t"]


def test_multiple_statements_are_rejected(tmp_path: Path) -> None:
    path = tmp_path / "two.sql"
    path.write_text("SELECT 1;\nSELECT 2;\n")
    with pytest.raises(sqlfiles.SqlFileError, match="exactly one"):
        sqlfiles.parse(path)


def test_unknown_directive_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "x.sql"
    path.write_text("-- lab:speed=fast\nSELECT 1;\n")
    with pytest.raises(sqlfiles.SqlFileError, match="unknown directive"):
        sqlfiles.parse(path)


def test_agent_prompt_keeps_escaped_quotes() -> None:
    agent = sqlfiles.parse(SQL_DIR / "06-agent" / "03-create-rebooking-agent.sql")
    assert "passenger''s final_destination" in agent.statement
    assert agent.statement.endswith("WITH ('max_iterations' = '10')")


def test_statement_name_rejects_invalid() -> None:
    with pytest.raises(sqlfiles.SqlFileError):
        sqlfiles.statement_name("x" * 101)
