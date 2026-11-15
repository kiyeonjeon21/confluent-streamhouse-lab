from __future__ import annotations

import json

import httpx
import pytest

from streamhouse_lab import sqlfiles, stages
from streamhouse_lab.flink import FlinkClient, FlinkError, fold_changelog
from tests.conftest import FAKE_SECRET

BASE = "/sql/v1/organizations/00000000-0000-0000-0000-000000000000/environments/env-test01"


class FakeFlink:
    """In-memory stand-in for the statements API, enough to exercise the client."""

    def __init__(self, phases=("PENDING", "COMPLETED"), results=None, columns=("a",)):
        self.statements: dict[str, dict] = {}
        self.phases = list(phases)
        self.results = results or [{"op": 0, "row": ["1"]}]
        self.columns = columns
        self.requests: list[httpx.Request] = []

    def body(self, name: str) -> dict:
        stmt = self.statements[name]
        return {
            "name": name,
            "spec": stmt["spec"],
            "status": {
                "phase": stmt["phases"][min(stmt["polls"], len(stmt["phases"]) - 1)],
                "detail": "boom" if "FAILED" in stmt["phases"] else "",
                "traits": {"sql_kind": "SELECT", "schema": {"columns": [{"name": c} for c in self.columns]}},
            },
        }

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path.removeprefix(BASE)
        if request.method == "POST" and path == "/statements":
            payload = json.loads(request.content)
            self.statements[payload["name"]] = {"spec": payload["spec"], "phases": self.phases, "polls": 0}
            return httpx.Response(200, json=self.body(payload["name"]))
        if path.endswith("/results"):
            name = path.split("/")[2]
            page = request.url.params.get("page")
            if page == "2":
                return httpx.Response(200, json={"metadata": {"next": ""}, "results": {"data": []}})
            data = self.results
            return httpx.Response(
                200,
                json={
                    "metadata": {"next": f"https://x{BASE}/statements/{name}/results?page=2"},
                    "results": {"data": data},
                },
            )
        if path.startswith("/statements/"):
            name = path.split("/")[2]
            if name not in self.statements:
                return httpx.Response(404, json={})
            if request.method == "DELETE":
                del self.statements[name]
                return httpx.Response(202)
            stmt = self.statements[name]
            response = httpx.Response(200, json=self.body(name))
            stmt["polls"] += 1
            return response
        if path == "/statements":
            data = [self.body(n) for n in self.statements]
            return httpx.Response(200, json={"data": data, "metadata": {"next": ""}})
        return httpx.Response(500)


def client_for(fake: FakeFlink, lab_config) -> FlinkClient:
    return FlinkClient(lab_config, transport=httpx.MockTransport(fake.handler), sleep=lambda _: None)


def test_submit_sets_catalog_database_pool_and_principal(lab_config) -> None:
    fake = FakeFlink()
    with client_for(fake, lab_config) as client:
        client.submit("lab-x", "SELECT 1", mode="snapshot")
    spec = fake.statements["lab-x"]["spec"]
    assert spec["properties"] == {
        "sql.current-catalog": "river-air-lab",
        "sql.current-database": "river-air",
        "sql.snapshot.mode": "now",
    }
    assert spec["compute_pool_id"] == "lfcp-test01"
    assert spec["principal"] == "sa-test01"


def test_streaming_mode_has_no_snapshot_property(lab_config) -> None:
    fake = FakeFlink()
    with client_for(fake, lab_config) as client:
        client.submit("lab-x", "SELECT 1")
    assert "sql.snapshot.mode" not in fake.statements["lab-x"]["spec"]["properties"]


def test_basic_auth_header_and_no_secret_in_urls(lab_config) -> None:
    fake = FakeFlink()
    with client_for(fake, lab_config) as client:
        client.submit("lab-x", "SELECT 1")
    request = fake.requests[0]
    assert request.headers["authorization"].startswith("Basic ")
    assert FAKE_SECRET not in str(request.url)


def test_query_waits_reads_pages_and_deletes(lab_config) -> None:
    fake = FakeFlink(results=[{"op": 0, "row": ["1"]}, {"op": 0, "row": ["2"]}])
    with client_for(fake, lab_config) as client:
        result = client.query("check-x", "SELECT a FROM t")
    assert result.columns == ["a"]
    assert result.rows == [["1"], ["2"]]
    assert result.records() == [{"a": "1"}, {"a": "2"}]
    assert "check-x" not in fake.statements


def test_failed_statement_raises_with_detail(lab_config) -> None:
    fake = FakeFlink(phases=("PENDING", "FAILED"))
    with client_for(fake, lab_config) as client:
        client.submit("lab-x", "SELECT 1")
        with pytest.raises(FlinkError, match="FAILED: boom"):
            client.wait("lab-x")


def test_wait_times_out(lab_config) -> None:
    fake = FakeFlink(phases=("PENDING",))
    ticks = iter(range(0, 10_000, 100))
    client = FlinkClient(
        lab_config,
        transport=httpx.MockTransport(fake.handler),
        sleep=lambda _: None,
        clock=lambda: next(ticks),
    )
    client.submit("lab-x", "SELECT 1")
    with pytest.raises(FlinkError, match="still PENDING"):
        client.wait("lab-x", timeout=250)


def test_fold_changelog_applies_updates_and_deletes() -> None:
    changes = [
        {"op": 0, "row": ["P-001", "OK"]},
        {"op": 1, "row": ["P-001", "OK"]},
        {"op": 2, "row": ["P-001", "AT_RISK"]},
        {"op": 0, "row": ["P-003", "NO_CONNECTION"]},
        {"op": 3, "row": ["P-003", "NO_CONNECTION"]},
    ]
    assert fold_changelog(changes) == [["P-001", "AT_RISK"]]


def test_apply_stage_is_idempotent(lab_config) -> None:
    fake = FakeFlink(phases=("PENDING", "COMPLETED"))
    stage = sqlfiles.stage("sources")
    with client_for(fake, lab_config) as client:
        first = stages.apply_stage(client, stage, log=lambda _: None)
        posts_after_first = sum(r.method == "POST" for r in fake.requests)
        second = stages.apply_stage(client, stage, log=lambda _: None)
    assert [a.skipped for a in first] == [False] * len(first)
    assert [a.skipped for a in second] == [True] * len(second)
    assert sum(r.method == "POST" for r in fake.requests) == posts_after_first
    assert next(a.name for a in first) == "lab-sources-01-create-flight-status"


def test_apply_stage_retries_a_failed_statement(lab_config) -> None:
    fake = FakeFlink(phases=("FAILED",))
    stage = sqlfiles.stage("sources")
    with client_for(fake, lab_config) as client:
        name = stage.statement_name(stage.statements()[0])
        client.submit(name, "broken")
        fake.phases = ["PENDING", "COMPLETED"]
        applied = stages.apply_stage(client, stage, log=lambda _: None)
    assert applied[0].skipped is False
    assert applied[0].phase == "COMPLETED"
