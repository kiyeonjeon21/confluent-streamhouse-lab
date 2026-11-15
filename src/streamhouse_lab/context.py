"""Client for the Real-Time Context Engine (RTCE), the MCP server in front of Lightning Tables.

The engine exposes three tools: listTopics, getMetadata, and queryData.
Every queryData call must set max_result_rows (1-200); queries time out after 20 s.
Reference: docs.confluent.io/cloud/current/lightning-tables/query/mcp.html
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from mcp import Client
from mcp.client.streamable_http import create_mcp_http_client, streamable_http_client
from mcp.shared.exceptions import MCPError

from streamhouse_lab.config import ConfigError, LabConfig
from streamhouse_lab.redact import ApiKey

MAX_RESULT_ROWS = 200


@dataclass
class ToolCall:
    tool: str
    arguments: dict[str, Any]
    is_error: bool
    payload: Any
    elapsed_ms: float


def reader_key(config: LabConfig) -> ApiKey:
    if config.context_reader is None:
        raise ConfigError(
            "No context-reader Global API key. Set lightning_topics in terraform.tfvars and redeploy."
        )
    return config.context_reader


def query_arguments(topic: str, query: str, max_result_rows: int = 50) -> dict[str, Any]:
    if not 1 <= max_result_rows <= MAX_RESULT_ROWS:
        raise ValueError(f"max_result_rows must be 1-{MAX_RESULT_ROWS}, got {max_result_rows}")
    return {"topic_name": topic, "query": query, "max_result_rows": max_result_rows}


def decode_content(result: Any) -> tuple[bool, Any]:
    """Turn an MCP CallToolResult into (is_error, payload), parsing JSON text blocks."""
    structured = getattr(result, "structured_content", None) or getattr(result, "structuredContent", None)
    is_error = bool(getattr(result, "is_error", None) or getattr(result, "isError", False))
    if structured is not None:
        return is_error, structured
    texts = [block.text for block in getattr(result, "content", []) or [] if hasattr(block, "text")]
    payload: Any = "\n".join(texts)
    try:
        payload = json.loads(payload)
    except (TypeError, ValueError):
        pass
    return is_error, payload


class Session:
    def __init__(self, client: Any) -> None:
        self._client = client

    async def tool_names(self) -> list[str]:
        tools = await self._client.list_tools()
        return [tool.name for tool in tools.tools]

    async def call(self, tool: str, arguments: dict[str, Any]) -> ToolCall:
        start = time.perf_counter()
        try:
            raw = await self._client.call_tool(tool, arguments)
        except MCPError as exc:
            # A JSON-RPC error instead of a tool result. Under load the engine answers this way
            # (observed 2026-11-15 at about 160 requests/s from us-east-1), so record it as data.
            elapsed = (time.perf_counter() - start) * 1000
            detail = f"{exc} {exc.data}" if exc.data is not None else str(exc)
            payload = {"status": "error", "error": {"code": f"MCP_{exc.code}", "message": detail[:300]}}
            return ToolCall(tool, arguments, True, payload, elapsed)
        elapsed = (time.perf_counter() - start) * 1000
        is_error, payload = decode_content(raw)
        return ToolCall(tool, arguments, is_error, payload, elapsed)

    async def query(self, topic: str, sql: str, max_result_rows: int = 50) -> ToolCall:
        return await self.call("queryData", query_arguments(topic, sql, max_result_rows))


def result_rows(payload: Any) -> list[list]:
    """Rows from a successful queryData payload: {"status", "rows": {"schema", "data": [{"row": [...]}]}}.

    Shape observed on 2026-11-15."""
    if not isinstance(payload, dict) or payload.get("status") != "success":
        return []
    return [item["row"] for item in (payload.get("rows") or {}).get("data") or []]


def error_code(payload: Any) -> str | None:
    if isinstance(payload, dict) and payload.get("status") == "error":
        return (payload.get("error") or {}).get("code") or "UNKNOWN"
    return None


class ContextEngine:
    """Opens one MCP session per `with` block. Use `run()` from synchronous code."""

    def __init__(self, config: LabConfig) -> None:
        self.endpoint = config.context_mcp_endpoint
        self._key = reader_key(config)

    def _transport(self):
        token = self._key.basic_token().reveal()
        http_client = create_mcp_http_client(headers={"Authorization": f"Basic {token}"})
        return streamable_http_client(self.endpoint, http_client=http_client)

    @asynccontextmanager
    async def session(self) -> AsyncIterator[Session]:
        """One MCP session, kept open for repeated calls (initialisation is not timed per call)."""
        async with Client(self._transport()) as client:
            yield Session(client)

    async def _session(self, calls: list[tuple[str, dict[str, Any]]]) -> tuple[list[str], list[ToolCall]]:
        async with self.session() as session:
            names = await session.tool_names()
            results = [await session.call(tool, arguments) for tool, arguments in calls]
            return names, results

    def run(self, calls: list[tuple[str, dict[str, Any]]]) -> tuple[list[str], list[ToolCall]]:
        """List the tools, then make each call in order within one session."""
        return asyncio.run(self._session(calls))
