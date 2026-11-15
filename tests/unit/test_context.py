from __future__ import annotations

from types import SimpleNamespace

import pytest

from streamhouse_lab import config
from streamhouse_lab.context import decode_content, query_arguments, reader_key


def test_query_arguments_require_row_cap() -> None:
    assert query_arguments("passenger_state", "SELECT * FROM passenger_state", 10) == {
        "topic_name": "passenger_state",
        "query": "SELECT * FROM passenger_state",
        "max_result_rows": 10,
    }
    for bad in (0, 201):
        with pytest.raises(ValueError):
            query_arguments("t", "SELECT 1", bad)


def test_reader_key_missing_is_actionable(lab_config) -> None:
    with pytest.raises(config.ConfigError, match="lightning_topics"):
        reader_key(lab_config)


def test_decode_prefers_structured_content() -> None:
    result = SimpleNamespace(structured_content={"rows": [1]}, is_error=False, content=[])
    assert decode_content(result) == (False, {"rows": [1]})


def test_decode_parses_json_text_and_errors() -> None:
    result = SimpleNamespace(
        content=[SimpleNamespace(text='{"error": "RATE_LIMIT_EXCEEDED"}')], is_error=True
    )
    assert decode_content(result) == (True, {"error": "RATE_LIMIT_EXCEEDED"})


def test_decode_keeps_plain_text() -> None:
    result = SimpleNamespace(content=[SimpleNamespace(text="not json")], is_error=False)
    assert decode_content(result) == (False, "not json")


def test_a_json_rpc_error_becomes_an_error_result() -> None:
    import asyncio

    from mcp.shared.exceptions import MCPError

    from streamhouse_lab.context import Session, error_code

    class Client:
        async def call_tool(self, tool, arguments):
            raise MCPError(code=-32603, message="Server returned an error response", data={"retry": True})

    call = asyncio.run(Session(Client()).query("t", "SELECT 1", 1))
    assert call.is_error
    assert error_code(call.payload) == "MCP_-32603"
    assert "retry" in call.payload["error"]["message"]
