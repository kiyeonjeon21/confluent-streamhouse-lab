"""Measurement plumbing for Phase 4: a JSON-lines recorder and a network probe.

Every event carries a monotonic timestamp (`mono_ns`) for durations and a wall
clock (`wall`) for correlating with Confluent's billing and metrics. Raw files
go to results/<experiment>/<run-id>.jsonl, which git ignores.
"""

from __future__ import annotations

import datetime as dt
import json
import socket
import ssl
import subprocess
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from streamhouse_lab.config import REPO_ROOT

RESULTS_DIR = REPO_ROOT / "results"


def git_revision() -> str:
    proc = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "rev-parse", "--short", "HEAD"], capture_output=True, text=True
    )
    dirty = subprocess.run(["git", "-C", str(REPO_ROOT), "diff", "--quiet"]).returncode != 0
    return proc.stdout.strip() + ("-dirty" if dirty else "")


class Recorder:
    """Append-only JSON-lines log for one experiment run.

    The first line is a `run` header with the metadata needed to interpret the
    numbers: experiment, parameters, region, client location, git revision.
    """

    def __init__(
        self,
        experiment: str,
        params: dict[str, Any],
        *,
        region: str,
        client_location: str,
        results_dir: Path = RESULTS_DIR,
        clock: Callable[[], int] = time.monotonic_ns,
        revision: Callable[[], str] = git_revision,
    ) -> None:
        self.run_id = f"{dt.datetime.now(dt.UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
        self.path = results_dir / experiment / f"{self.run_id}.jsonl"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock
        self._file = self.path.open("a")
        self.event(
            "run",
            experiment=experiment,
            params=params,
            region=region,
            client_location=client_location,
            revision=revision(),
        )

    def now(self) -> int:
        return self._clock()

    def event(self, kind: str, **fields: Any) -> None:
        line = {"kind": kind, "mono_ns": self._clock(), "wall": dt.datetime.now(dt.UTC).isoformat(), **fields}
        self._file.write(json.dumps(line, default=str) + "\n")
        self._file.flush()

    def close(self) -> None:
        self._file.close()

    def __enter__(self) -> Recorder:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def probe_network(host: str, port: int = 443, timeout: float = 5.0) -> dict[str, float]:
    """Time DNS, TCP connect, and TLS handshake to one host, in milliseconds.

    TCP connect time is roughly one network round trip, so it is the figure to
    report beside every latency measured against the same host.
    """
    t0 = time.perf_counter()
    family, kind, proto, _, address = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)[0]
    t1 = time.perf_counter()
    raw = socket.socket(family, kind, proto)
    raw.settimeout(timeout)
    try:
        raw.connect(address)
        t2 = time.perf_counter()
        context = ssl.create_default_context()
        with context.wrap_socket(raw, server_hostname=host):
            t3 = time.perf_counter()
    finally:
        raw.close()
    return {
        "dns_ms": (t1 - t0) * 1000,
        "tcp_connect_ms": (t2 - t1) * 1000,
        "tls_handshake_ms": (t3 - t2) * 1000,
    }
