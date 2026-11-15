"""Client for the Real-Time Context Engine (RTCE), the MCP server in front of Lightning Tables.

The engine exposes three tools: listTopics, getMetadata, and queryData.
Every queryData call must set max_result_rows (1-200); queries time out after 20 s.
Reference: docs.confluent.io/cloud/current/lightning-tables/query/mcp.html
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any

from mcp import Client
from mcp.client.streamable_http import create_mcp_http_client, streamable_http_client

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


class ContextEngine:
    """Opens one MCP session per `with` block. Use `run()` from synchronous code."""

    def __init__(self, config: LabConfig) -> None:
        self.endpoint = config.context_mcp_endpoint
        self._key = reader_key(config)

    def _transport(self):
        token = self._key.basic_token().reveal()
        http_client = create_mcp_http_client(headers={"Authorization": f"Basic {token}"})
        return streamable_http_client(self.endpoint, http_client=http_client)

    async def _session(self, calls: list[tuple[str, dict[str, Any]]]) -> tuple[list[str], list[ToolCall]]:
        async with Client(self._transport()) as client:
            tools = await client.list_tools()
            names = [tool.name for tool in tools.tools]
            results = []
            for tool, arguments in calls:
                start = time.perf_counter()
                raw = await client.call_tool(tool, arguments)
                elapsed = (time.perf_counter() - start) * 1000
                is_error, payload = decode_content(raw)
                results.append(ToolCall(tool, arguments, is_error, payload, elapsed))
            return names, results

    def run(self, calls: list[tuple[str, dict[str, Any]]]) -> tuple[list[str], list[ToolCall]]:
        """List the tools, then make each call in order within one session."""
        return asyncio.run(self._session(calls))
