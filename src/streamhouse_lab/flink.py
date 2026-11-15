"""Minimal client for the Confluent Cloud Flink statements API (sql/v1, GA).

Reference: docs.confluent.io/cloud/current/flink/operate-and-deploy/flink-rest-api.html
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

import httpx

from streamhouse_lab.config import LabConfig
from streamhouse_lab.redact import ApiKey

TERMINAL_FAILURE = {"FAILED", "FAILING", "DELETING"}

# Result rows carry a changelog operation.
OP_INSERT, OP_UPDATE_BEFORE, OP_UPDATE_AFTER, OP_DELETE = 0, 1, 2, 3


class FlinkError(RuntimeError):
    pass


@dataclass
class Statement:
    name: str
    phase: str
    detail: str
    sql_kind: str | None
    columns: list[str] = field(default_factory=list)
    raw: dict = field(default_factory=dict, repr=False)

    @classmethod
    def from_api(cls, body: dict) -> Statement:
        status = body.get("status") or {}
        traits = status.get("traits") or {}
        schema = traits.get("schema") or {}
        return cls(
            name=body["name"],
            phase=status.get("phase", "UNKNOWN"),
            detail=status.get("detail", ""),
            sql_kind=traits.get("sql_kind"),
            columns=[c["name"] for c in schema.get("columns") or []],
            raw=body,
        )


@dataclass
class MaterializedTable:
    name: str
    phase: str
    version: int | None
    stopped: bool
    warnings: list[str] = field(default_factory=list)
    table_options: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_api(cls, body: dict) -> MaterializedTable:
        status = body.get("status") or {}
        spec = body.get("spec") or {}
        return cls(
            name=body["name"],
            phase=status.get("phase", "UNKNOWN"),
            version=status.get("version"),
            stopped=bool(spec.get("stopped")),
            warnings=[f"{w.get('severity')}: {w.get('reason')}" for w in status.get("warnings") or []],
            table_options=spec.get("table_options") or {},
        )


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[list]

    def records(self) -> list[dict]:
        return [dict(zip(self.columns, row, strict=False)) for row in self.rows]


def fold_changelog(changes: Iterable[dict]) -> list[list]:
    """Apply a stream of {op, row} changes to produce the current result set."""
    rows: list[list] = []
    for change in changes:
        op, row = change.get("op", OP_INSERT), change["row"]
        if op in (OP_INSERT, OP_UPDATE_AFTER):
            rows.append(row)
        elif op in (OP_UPDATE_BEFORE, OP_DELETE):
            try:
                rows.remove(row)
            except ValueError:
                pass
        else:
            raise FlinkError(f"unknown changelog op {op!r}")
    return rows


class FlinkClient:
    def __init__(
        self,
        config: LabConfig,
        *,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        key: ApiKey = config.operator_flink
        self._http = httpx.Client(
            base_url=(
                f"{config.flink_rest_endpoint}/sql/v1/organizations/{config.organization_id}"
                f"/environments/{config.environment_id}"
            ),
            auth=httpx.BasicAuth(key.key, key.secret.reveal()),
            timeout=30.0,
            transport=transport,
        )
        self._sleep = sleep
        self._clock = clock

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> FlinkClient:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- HTTP -----------------------------------------------------------------

    def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        for attempt in range(5):
            response = self._http.request(method, url, **kwargs)
            if response.status_code == 429 or response.status_code >= 500:
                self._sleep(min(2**attempt, 10))
                continue
            return response
        return response

    @staticmethod
    def _raise_for(response: httpx.Response, action: str) -> None:
        if response.is_success:
            return
        # The body is the API's error message. Requests never echo credentials.
        raise FlinkError(f"{action} failed: HTTP {response.status_code}: {response.text[:500]}")

    # -- statements -----------------------------------------------------------

    def properties(self, mode: str) -> dict[str, str]:
        props = {
            "sql.current-catalog": self.config.flink_catalog,
            "sql.current-database": self.config.flink_database,
        }
        if mode == "snapshot":
            props["sql.snapshot.mode"] = "now"
        return props

    def submit(self, name: str, sql: str, *, mode: str = "streaming") -> Statement:
        body = {
            "name": name,
            "organization_id": self.config.organization_id,
            "environment_id": self.config.environment_id,
            "spec": {
                "statement": sql,
                "properties": self.properties(mode),
                "compute_pool_id": self.config.flink_compute_pool_id,
                "principal": self.config.operator_id,
                "stopped": False,
            },
        }
        response = self._request("POST", "/statements", json=body)
        self._raise_for(response, f"submit {name}")
        return Statement.from_api(response.json())

    def get(self, name: str) -> Statement | None:
        response = self._request("GET", f"/statements/{name}")
        if response.status_code == 404:
            return None
        self._raise_for(response, f"get {name}")
        return Statement.from_api(response.json())

    def list(self, prefix: str = "") -> list[Statement]:
        found: list[Statement] = []
        url: str | None = "/statements?page_size=100"
        while url:
            response = self._request("GET", url)
            self._raise_for(response, "list statements")
            body = response.json()
            found.extend(Statement.from_api(item) for item in body.get("data") or [])
            url = (body.get("metadata") or {}).get("next") or None
        return [s for s in found if s.name.startswith(prefix)]

    def materialized_tables(self) -> list[MaterializedTable]:
        """Materialized tables in the environment. Their continuous queries do not appear
        in the statement list; the CREATE statement itself completes as a bounded job."""
        found: list[MaterializedTable] = []
        url: str | None = "/materialized-tables?page_size=100"
        while url:
            response = self._request("GET", url)
            self._raise_for(response, "list materialized tables")
            body = response.json()
            found.extend(MaterializedTable.from_api(item) for item in body.get("data") or [])
            url = (body.get("metadata") or {}).get("next") or None
        return found

    def materialized_table(self, name: str) -> dict:
        """The raw materialized table resource: spec, status, and metadata.resource_version."""
        url = f"/databases/{self.config.kafka_cluster_id}/materialized-tables/{name}"
        response = self._request("GET", url)
        self._raise_for(response, f"get materialized table {name}")
        return response.json()

    def set_materialized_table_stopped(self, name: str, stopped: bool, *, attempts: int = 5) -> dict:
        """Stop or resume a materialized table. Resuming restarts its query from the last
        committed offset with its state; the PUT needs the current resource_version, and a
        stale one returns 409, so read, change, and retry."""
        url = f"/databases/{self.config.kafka_cluster_id}/materialized-tables/{name}"
        for _ in range(attempts):
            current = self.materialized_table(name)
            body = {
                "name": name,
                "spec": {**(current.get("spec") or {}), "stopped": stopped},
                "metadata": {"resource_version": (current.get("metadata") or {}).get("resource_version")},
            }
            response = self._request("PUT", url, json=body)
            if response.status_code == 409:
                continue
            self._raise_for(response, f"{'stop' if stopped else 'resume'} materialized table {name}")
            return response.json()
        raise FlinkError(f"{name}: resource_version kept changing; gave up after {attempts} attempts")

    def delete(self, name: str) -> None:
        response = self._request("DELETE", f"/statements/{name}")
        if response.status_code != 404:
            self._raise_for(response, f"delete {name}")

    def wait(
        self,
        name: str,
        *,
        until: frozenset[str] = frozenset({"COMPLETED"}),
        timeout: float = 300.0,
        poll: float = 2.0,
    ) -> Statement:
        deadline = self._clock() + timeout
        while True:
            statement = self.get(name)
            if statement is None:
                raise FlinkError(f"statement {name} disappeared while waiting")
            if statement.phase in until:
                return statement
            if statement.phase in TERMINAL_FAILURE:
                raise FlinkError(f"statement {name} is {statement.phase}: {statement.detail}")
            if self._clock() >= deadline:
                raise FlinkError(
                    f"statement {name} still {statement.phase} after {timeout:.0f}s: {statement.detail}"
                )
            self._sleep(poll)

    def results(self, name: str) -> QueryResult:
        """Read every result page of a bounded statement and fold the changelog."""
        statement = self.get(name)
        if statement is None:
            raise FlinkError(f"statement {name} not found")
        changes: list[dict] = []
        url: str | None = f"/statements/{name}/results"
        while url:
            response = self._request("GET", url)
            self._raise_for(response, f"results {name}")
            body = response.json()
            page = (body.get("results") or {}).get("data") or []
            changes.extend(page)
            # A completed statement can keep returning a `next` link with empty pages.
            url = (body.get("metadata") or {}).get("next") or None if page else None
        return QueryResult(columns=statement.columns, rows=fold_changelog(changes))

    def query(self, name: str, sql: str, *, timeout: float = 300.0, keep: bool = False) -> QueryResult:
        """Run a snapshot query to completion, read its rows, and delete it unless asked to keep it."""
        self.submit(name, sql, mode="snapshot")
        try:
            self.wait(name, until=frozenset({"COMPLETED"}), timeout=timeout)
            return self.results(name)
        finally:
            if not keep:
                self.delete(name)
