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
    # Keep one physical line per row: model output often contains newlines.
    cells = [[("NULL" if v is None else str(v).replace("\n", "\\n")) for v in row] for row in result.rows]
    widths = [max([len(c), *(len(r[i]) for r in cells)]) for i, c in enumerate(columns)]
    for line in [columns, *cells]:
        print("  ".join(v.ljust(w) for v, w in zip(line, widths, strict=False)).rstrip())
    print(f"({len(cells)} row{'' if len(cells) == 1 else 's'})")


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
        print("\nmaterialized tables (continuous queries; billed while RUNNING):")
        for table in client.materialized_tables():
            stopped = " stopped" if table.stopped else ""
            print(f"  {table.phase:10} {table.name} v{table.version}{stopped}")
            for warning in table.warnings:
                print(f"             {warning}")
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


def cmd_iceberg(args: argparse.Namespace) -> int:
    """Read a Tableflow table straight from Iceberg, without Flink or Kafka."""
    from streamhouse_lab import iceberg  # pyiceberg is heavy; import only when used

    table = iceberg.load(config.load(), args.table)
    snaps = iceberg.snapshots(table)
    print(f"format v{table.format_version}, {len(snaps)} snapshots")
    for snap in snaps[-5:]:
        print(f"  {snap.committed_at:%Y-%m-%dT%H:%M:%SZ}  {snap.operation}  {snap.snapshot_id}")
    records = iceberg.rows(table)
    columns = [*iceberg.business_columns(table), f"{iceberg.METADATA_PREFIX}offset"]
    print()
    print_table(QueryResult(columns=columns, rows=[[r[c] for c in columns] for r in records]))
    return 0


def cmd_bench(args: argparse.Namespace) -> int:
    """Phase 4 tooling. Everything except --dry-run and summarize writes to or reads from the cloud."""
    import asyncio
    import urllib.parse

    from streamhouse_lab import bench, datagen, measure, summarize

    if args.action == "summarize":
        header, summaries = summarize.summarize_file(Path(args.file), args.metric)
        print(f"run {Path(args.file).stem}: {header['experiment']} {header['params']}")
        print(f"region {header['region']}, client {header['client_location']}, revision {header['revision']}")
        print(summarize.to_markdown(summaries))
        return 0

    dataset = datagen.generate(args.passengers, args.seed)
    if args.action == "gen" and args.dry_run:
        print(f"{len(dataset.flights)} flights, {len(dataset.itineraries)} passengers (seed {args.seed})")
        print(f"expected risk: {datagen.expected_risk(dataset)}")
        return 0

    cfg = config.load()
    if args.action == "gen":
        from streamhouse_lab.kafka_io import BenchProducer

        acks = BenchProducer.connect(cfg).send(dataset.records())
        print(f"produced {len(acks)} records; expected risk {datagen.expected_risk(dataset)}")
        return 0

    # The hybrid experiment never touches Lightning, so it must not need the context reader.
    engine = ContextEngine(cfg) if args.action != "hybrid" else None
    host = urllib.parse.urlsplit(cfg.context_mcp_endpoint).hostname or ""
    params = {k: v for k, v in vars(args).items() if k not in ("func", "command", "action")}
    with measure.Recorder(
        args.action, params, region=cfg.cloud_region, client_location=args.location
    ) as recorder:
        recorder.event("net", phase="before", host=host, **measure.probe_network(host))

        async def run() -> None:
            if args.action == "e1":
                from streamhouse_lab.kafka_io import BenchProducer

                producer = BenchProducer.connect(cfg)
                async with engine.session() as session:
                    await bench.run_freshness(
                        session, producer, recorder, datagen.probe_flight(dataset),
                        samples=args.samples, poll_interval=args.poll, gap=args.gap,
                        jitter=args.jitter, seed=args.seed,
                    )  # fmt: skip
            elif args.action == "e3":
                async with engine.session() as session:
                    plan = bench.query_shapes(args.passengers, args.seed, args.repeat)
                    await bench.run_query_latency(session, recorder, plan)
            elif args.action == "e4":
                levels = [int(x) for x in args.levels.split(",")]
                await bench.run_concurrency(
                    engine.session, recorder, levels=levels, duration=args.duration,
                    passengers=args.passengers, seed=args.seed,
                )  # fmt: skip
            elif args.action == "e2":
                from streamhouse_lab.kafka_io import BenchProducer, TopicReader

                producer = BenchProducer.connect(cfg)
                watchers = {
                    level: bench.TopicWatcher(
                        TopicReader.connect(cfg, bench.STATE_TOPIC, isolation=level), bench.probe_marker
                    ).start()
                    for level in ("read_uncommitted", "read_committed")
                }
                try:
                    async with engine.session() as session:
                        await bench.run_stage_latency(
                            session, producer, watchers, recorder, datagen.probe_flight(dataset),
                            samples=args.samples, poll_interval=args.poll, gap=args.gap,
                            jitter=args.jitter, seed=args.seed,
                        )  # fmt: skip
                finally:
                    for watcher in watchers.values():
                        watcher.stop()
            elif args.action == "hybrid":
                run_hybrid_experiment(cfg, recorder, resume=args.resume)

        asyncio.run(run())
        recorder.event("net", phase="after", host=host, **measure.probe_network(host))
    print(f"raw results: {recorder.path.relative_to(config.REPO_ROOT)}")
    metrics = {
        "e1": ["seen_by_ms"],
        "e2": ["read_uncommitted_ms", "read_committed_ms", "lightning_seen_by_ms"],
        "hybrid": [],
    }.get(args.action, ["latency_ms"])
    for metric in metrics:
        print(f"\n{metric}")
        print(summarize.to_markdown(summarize.summarize_file(recorder.path, metric)[1]))
    return 0


def run_hybrid_experiment(cfg: config.LabConfig, recorder, *, resume: bool = False) -> None:
    """Wire the hybrid-read experiment to the real Kafka, Iceberg, and Flink."""
    from streamhouse_lab import bench, iceberg, kafka_io

    table = bench.HYBRID_TABLE
    check = sqlfiles.parse(sqlfiles.stage("hybrid").path / "checks" / "hybrid-history.sql")

    with FlinkClient(cfg) as client:

        def flink_ids() -> list[str]:
            name = sqlfiles.statement_name("check", "hybrid", str(int(time.time() * 1000)))
            return [str(r["flight_id"]) for r in client.query(name, check.statement).records()]

        steps = bench.HybridSteps(
            write_old=lambda: stages.run_event(client, "hybrid-old-batch"),
            write_new=lambda: stages.run_event(client, "hybrid-new-batch"),
            kafka_ids=lambda: [r.value["flight_id"] for r in kafka_io.read_all(cfg, table) if r.value],
            iceberg_ids=lambda: [r["flight_id"] for r in iceberg.rows(iceberg.load(cfg, table))],
            flink_ids=flink_ids,
            truncate=lambda: kafka_io.truncate(cfg, table),
        )
        result = bench.run_hybrid(steps, recorder, resume=resume)
    print(result)


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

    p = sub.add_parser("iceberg", help="read a Tableflow table directly from Iceberg (no Flink)")
    p.add_argument("table")
    p.set_defaults(func=cmd_iceberg)

    p = sub.add_parser("bench", help="Phase 4: generate data, run experiments, summarize results")
    p.add_argument("action", choices=["gen", "e1", "e2", "e3", "e4", "hybrid", "summarize"])
    p.add_argument("file", nargs="?", help="results file for summarize")
    p.add_argument("--passengers", type=int, default=10_000)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--dry-run", action="store_true", help="gen: print the dataset shape, write nothing")
    p.add_argument("--location", default="unspecified", help="where the client runs, e.g. seoul-laptop")
    p.add_argument("--samples", type=int, default=30, help="e1, e2: freshness samples")
    p.add_argument("--poll", type=float, default=0.25, help="e1, e2: seconds between polls")
    p.add_argument("--gap", type=float, default=5.0, help="e1, e2: minimum seconds between samples")
    p.add_argument(
        "--jitter",
        type=float,
        default=60.0,
        help="e1: extra uniform random wait, at least the commit interval",
    )
    p.add_argument("--repeat", type=int, default=100, help="e3: rounds of every query shape")
    p.add_argument("--levels", default="1,2,4,8,16,32", help="e4: concurrency levels")
    p.add_argument("--duration", type=float, default=30.0, help="e4: seconds per level")
    p.add_argument("--metric", default="latency_ms", help="summarize: field to summarize")
    p.add_argument("--resume", action="store_true", help="hybrid: start after the old batch was truncated")
    p.set_defaults(func=cmd_bench)

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
