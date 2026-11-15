"""Runs the rebooking agent's task in Claude Code, as an external MCP client (experiment E5).

Claude Code gets the same instructions as the in-Flink agent (its system prompt is
the agent's prompt, read from the SQL file), the same model, and only the same two
Real-Time Context Engine tools. Built-in tools are disabled and no other MCP server
is loaded, so the comparison is between harnesses, not between models or tools.

The context reader's credential reaches Claude Code only through an environment
variable that the MCP config references; the config file itself holds no secret.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import time
from collections.abc import Iterable
from pathlib import Path

from streamhouse_lab.bench import AgentRun
from streamhouse_lab.config import SQL_DIR, LabConfig
from streamhouse_lab.context import reader_key

MODEL = "claude-sonnet-4-6"  # the model rebooking_model uses (sql/06-agent/01-create-rebooking-model.sql)
SERVER = "rtce"
TOOLS = (f"mcp__{SERVER}__queryData", f"mcp__{SERVER}__getMetadata")
AUTH_ENV = "LAB_RTCE_AUTHORIZATION"
AGENT_SQL = SQL_DIR / "06-agent" / "03-create-rebooking-agent.sql"
PROMPT_RE = re.compile(r"USING PROMPT '((?:[^']|'')*)'", re.S)
# The same task text AI_RUN_AGENT builds for P-001 in sql/06-agent/checks/rebooking-offers.sql.
TASK = "passenger_id=P-001; final_destination=SEA; inbound_arrival=2026-11-04 14:50:00.000"


def agent_prompt(path: Path = AGENT_SQL) -> str:
    match = PROMPT_RE.search(path.read_text())
    if not match:
        raise ValueError(f"no USING PROMPT '...' in {path}")
    return match.group(1).replace("''", "'")


def mcp_config(endpoint: str) -> dict:
    return {
        "mcpServers": {
            SERVER: {
                "type": "http",
                "url": endpoint,
                "headers": {"Authorization": f"${{{AUTH_ENV}}}"},
            }
        }
    }


def command(config_path: Path, prompt: str, task: str = TASK, model: str = MODEL) -> list[str]:
    return [
        "claude",
        "-p",
        task,
        "--model",
        model,
        "--system-prompt",
        prompt,
        "--tools",
        "",
        "--strict-mcp-config",
        "--mcp-config",
        str(config_path),
        "--allowedTools",
        ",".join(TOOLS),
        "--setting-sources",
        "project",
        "--output-format",
        "stream-json",
        "--verbose",
        "--max-budget-usd",
        "1",
    ]


def parse_stream(lines: Iterable[str]) -> AgentRun:
    """Turn Claude Code's stream-json output into an AgentRun.

    Tool calls are counted from tool_use blocks in assistant messages; the final
    `result` event carries the answer, turns, duration, and cost."""
    tool_calls, result = 0, None
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if event.get("type") == "assistant":
            content = (event.get("message") or {}).get("content") or []
            tool_calls += sum(1 for block in content if block.get("type") == "tool_use")
        elif event.get("type") == "result":
            result = event
    if result is None:
        return AgentRun(responses=[], elapsed_ms=0.0, tool_calls=tool_calls, error="NO_RESULT")
    return AgentRun(
        responses=[result.get("result") or ""],
        elapsed_ms=float(result.get("duration_ms") or 0.0),
        tool_calls=tool_calls,
        turns=result.get("num_turns"),
        cost_usd=result.get("total_cost_usd"),
        error=(result.get("subtype") if result.get("is_error") else None),
    )


def run(config: LabConfig, *, timeout: float = 300.0) -> AgentRun:
    token = reader_key(config).basic_token().reveal()
    with tempfile.TemporaryDirectory(prefix="lab-claude-") as workdir:
        config_path = Path(workdir) / "mcp.json"
        config_path.write_text(json.dumps(mcp_config(config.context_mcp_endpoint)))
        config_path.chmod(0o600)
        env = {**os.environ, AUTH_ENV: f"Basic {token}"}
        started = time.perf_counter()
        try:
            proc = subprocess.run(
                command(config_path, agent_prompt()),
                cwd=workdir,
                env=env,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return AgentRun(responses=[], elapsed_ms=timeout * 1000, error="TIMEOUT")
        wall_ms = (time.perf_counter() - started) * 1000
    outcome = parse_stream(proc.stdout.splitlines())
    if proc.returncode != 0 and outcome.error is None:
        outcome.error = f"EXIT_{proc.returncode}"
    # Wall time includes Claude Code's start-up, as the Flink agent's includes snapshot start-up.
    outcome.elapsed_ms = wall_ms
    return outcome
