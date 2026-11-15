"""`uv run lab` - drive the River Air Streamhouse lab from the terminal.

Every command that talks to Confluent Cloud reads IDs and credentials from
Terraform outputs and never prints a secret.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from streamhouse_lab import agent_connection, config, sqlfiles, stages
from streamhouse_lab.context import ContextEngine, query_arguments
from streamhouse_lab.flink import FlinkClient, QueryResult


def print_table(result: QueryResult) -> None:
    columns = result.columns or [f"col{i}" for i in range(len(result.rows[0]) if result.rows else 0)]
    cells = [[("NULL" if v is None else str(v)) for v in row] for row in result.rows]
    widths = [max([len(c), *(len(r[i]) for r in cells)]) for i, c in enumerate(columns)]
    print("  ".join(c.ljust(w) for c, w in zip(columns, widths, strict=True)))
    for row in cells:
        print("  ".join(v.ljust(w) for v, w in zip(row, widths, strict=False)))
    print(f"({len(cells)} rows)")


def cmd_doctor(_: argparse.Namespace) -> int:
    """Check local prerequisites without printing any credential."""
    ok = True

    def report(label: str, passed: bool, detail: str = "") -> None:
        nonlocal ok
        ok &= passed
        print(f"{'ok  ' if passed else 'FAIL'}  {label}{f' - {detail}' if detail else ''}")

    for tool in ("terraform", "confluent", "gh", "uv"):
        report(f"{tool} on PATH", shutil.which(tool) is not None)

    ctx = subprocess.run(["confluent", "context", "describe", "-o", "json"], capture_output=True, text=True)
    report("confluent CLI logged in", ctx.returncode == 0, "run `confluent login`" if ctx.returncode else "")

    gh = subprocess.run(["gh", "auth", "status"], capture_output=True, text=True)
    report("gh authenticated", gh.returncode == 0)

    for var in ("CONFLUENT_CLOUD_API_KEY", "CONFLUENT_CLOUD_API_SECRET"):
        present = bool(os.environ.get(var))
        report(f"{var} set for Terraform", present, "" if present else "see .env.example")

    def owner_only(path: Path) -> None:
        mode = path.stat().st_mode & 0o777
        private = mode & 0o077 == 0
        report(f"{path.name} is owner-only", private, "" if private else f"mode {mode:o}; run chmod 600")

    env_file = config.REPO_ROOT / ".env"
    if env_file.exists():
        owner_only(env_file)

    state = config.TERRAFORM_DIR / "terraform.tfstate"
    report("Terraform state present", state.exists(), "" if state.exists() else "not deployed yet")
    if state.exists():
        owner_only(state)
    return 0 if ok else 1


def cmd_status(_: argparse.Namespace) -> int:
    cfg = config.load()
    public = {
        "organization": cfg.organization_id,
        "environment": f"{cfg.flink_catalog} ({cfg.environment_id})",
        "cluster": f"{cfg.flink_database} ({cfg.kafka_cluster_id}, {cfg.kafka_cluster_type})",
        "region": cfg.cloud_region,
        "compute_pool": cfg.flink_compute_pool_id,
        "lightning_topics": list(cfg.lightning_topics),
        "tableflow_topics": list(cfg.tableflow_topics),
        "context_reader": "present" if cfg.context_reader else "absent",
    }
    for key, value in public.items():
        print(f"{key:18} {value}")
    with FlinkClient(cfg) as client:
        print("\nstatements:")
        for statement in sorted(client.list(), key=lambda s: s.name):
            print(f"  {statement.phase:10} {statement.name}")
    return 0


def cmd_apply(args: argparse.Namespace) -> int:
    stage = sqlfiles.stage(args.stage)
    with FlinkClient(config.load()) as client:
        stages.apply_stage(client, stage)
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    stage = sqlfiles.stage(args.stage)
    with FlinkClient(config.load()) as client:
        for name, result in stages.run_checks(client, stage).items():
            print(f"\n== {stage.name}/{name}")
            print_table(result)
    return 0


def cmd_event(args: argparse.Namespace) -> int:
    known = sqlfiles.events()
    if args.name not in known:
        print(f"unknown event {args.name!r}; known: {', '.join(known)}", file=sys.stderr)
        return 2
    with FlinkClient(config.load()) as client:
        statement = stages.run_event(client, args.name)
    print(f"{statement.name} {statement.phase} at {time.strftime('%Y-%m-%dT%H:%M:%S%z')}")
    return 0


def cmd_query(args: argparse.Namespace) -> int:
    sql = sqlfiles.parse(Path(args.file)).statement if args.file else args.sql
    name = sqlfiles.statement_name("adhoc", str(int(time.time())))
    with FlinkClient(config.load()) as client:
        print_table(client.query(name, sql))
    return 0


def cmd_context(args: argparse.Namespace) -> int:
    engine = ContextEngine(config.load())
    calls = []
    if args.action == "topics":
        calls = [("listTopics", {})]
    elif args.action == "metadata":
        calls = [("getMetadata", {"topic_name": args.topic})]
    elif args.action == "query":
        calls = [("queryData", query_arguments(args.topic, args.query, args.rows))]
    tools, results = engine.run(calls)
    print(f"tools: {', '.join(tools)}")
    for call in results:
        print(f"\n{call.tool} ({call.elapsed_ms:.0f} ms){' ERROR' if call.is_error else ''}")
        print(
            json.dumps(call.payload, indent=2, default=str)
            if not isinstance(call.payload, str)
            else call.payload
        )
    return 1 if any(c.is_error for c in results) else 0


def cmd_agent_connection(args: argparse.Namespace) -> int:
    cfg = config.load()
    if args.action == "create":
        found = agent_connection.create(cfg)
    elif args.action == "delete":
        return 0 if agent_connection.delete(cfg) else 1
    else:
        found = agent_connection.describe(cfg)
        if found is None:
            print("absent")
            return 1
    # Print only non-sensitive fields; never the raw describe output.
    print({k: found.get(k) for k in ("name", "type", "endpoint", "transport_type") if k in found})
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lab", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("doctor", help="check local tools, logins, and secret hygiene").set_defaults(
        func=cmd_doctor
    )
    sub.add_parser("status", help="show deployed IDs and Flink statements").set_defaults(func=cmd_status)

    stage_names = [s.name for s in sqlfiles.stages()]
    p = sub.add_parser("apply", help="apply a SQL stage (idempotent)")
    p.add_argument("stage", choices=stage_names)
    p.set_defaults(func=cmd_apply)

    p = sub.add_parser("check", help="run a stage's snapshot checks and print the rows")
    p.add_argument("stage", choices=stage_names)
    p.set_defaults(func=cmd_check)

    p = sub.add_parser("event", help="replay a business event from sql/events")
    p.add_argument("name")
    p.set_defaults(func=cmd_event)

    p = sub.add_parser("query", help="run an ad-hoc snapshot query")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--sql")
    group.add_argument("--file")
    p.set_defaults(func=cmd_query)

    p = sub.add_parser("context", help="call the Real-Time Context Engine over MCP")
    p.add_argument("action", choices=["topics", "metadata", "query"])
    p.add_argument("topic", nargs="?")
    p.add_argument("query", nargs="?", help='SQL for queryData, e.g. "SELECT * FROM passenger_state LIMIT 5"')
    p.add_argument("--rows", type=int, default=50, help="max_result_rows (1-200)")
    p.set_defaults(func=cmd_context)

    p = sub.add_parser("agent-connection", help="manage the agent's MCP connection to the context engine")
    p.add_argument("action", choices=["create", "show", "delete"])
    p.set_defaults(func=cmd_agent_connection)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "context" and args.action in ("metadata", "query") and not args.topic:
        print("context metadata/query needs a topic", file=sys.stderr)
        return 2
    if args.command == "context" and args.action == "query" and not args.query:
        print("context query needs a SQL query", file=sys.stderr)
        return 2
    try:
        return args.func(args)
    except (config.ConfigError, sqlfiles.SqlFileError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
