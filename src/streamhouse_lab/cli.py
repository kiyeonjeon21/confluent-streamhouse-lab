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
                probe = datagen.probe_flight(dataset)
                where = (
                    {"topic": bench.SOURCE_TOPIC, "query": bench.source_probe_query(probe.key["flight_id"])}
                    if args.read == "source"
                    else {}
                )
                async with engine.session() as session:
                    await bench.run_freshness(
                        session, producer, recorder, probe,
                        samples=args.samples, poll_interval=args.poll, gap=args.gap,
                        jitter=args.jitter, seed=args.seed, **where,
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
            elif args.action == "e5":
                await run_agent_experiment(cfg, engine, recorder, args)
            elif args.action == "e6":
                await asyncio.to_thread(run_failure_experiment, cfg, engine, recorder, dataset, args.scenario)

        asyncio.run(run())
        recorder.event("net", phase="after", host=host, **measure.probe_network(host))
    print(f"raw results: {recorder.path.relative_to(config.REPO_ROOT)}")
    if args.action == "e5":
        print(summarize.agent_table(recorder.path))
        return 0
    if args.action == "e6":
        return 0
    metrics = {
        "e1": ["seen_by_ms"],
        "e2": ["read_uncommitted_ms", "read_committed_ms", "lightning_seen_by_ms"],
        "hybrid": [],
    }.get(args.action, ["latency_ms"])
    for metric in metrics:
        print(f"\n{metric}")
        print(summarize.to_markdown(summarize.summarize_file(recorder.path, metric)[1]))
    return 0


async def run_agent_experiment(cfg: config.LabConfig, engine: ContextEngine, recorder, args) -> None:
    """Wire E5 to the real Flink agent, Claude Code, and the RA214 events."""
    from streamhouse_lab import bench, external_agent

    checks = sqlfiles.stage("agent-log").path / "checks"
    check = sqlfiles.parse(checks / "rebooking-offers-logged.sql")
    log_counts = sqlfiles.parse(checks / "agent-log-counts.sql")

    def flink_agent(client: FlinkClient) -> bench.AgentRun:
        name = sqlfiles.statement_name("check", "e5", str(int(time.time() * 1000)))
        started = time.perf_counter()
        try:
            records = client.query(name, check.statement).records()
        except Exception as exc:  # recorded per cycle; one failed run must not end the experiment
            return bench.AgentRun(
                [], (time.perf_counter() - started) * 1000, run_id=name, error=str(exc)[:300]
            )
        ordered = sorted(records, key=lambda r: str(r.get("passenger_id")))
        return bench.AgentRun(
            responses=[str(r.get("response") or "") for r in ordered],
            elapsed_ms=(time.perf_counter() - started) * 1000,
            run_id=name,
            error=None if all(r.get("agent_status") == "SUCCESS" for r in ordered) else "AGENT_STATUS",
        )

    with FlinkClient(cfg) as events_client, FlinkClient(cfg) as agent_client:
        agents = {} if args.external_only else {"flink": lambda: flink_agent(agent_client)}
        if args.external == "claude-code":
            agents["claude_code"] = lambda: external_agent.run(cfg)
        steps = bench.AgentSteps(
            reset=lambda: stages.run_event(events_client, "ra214-on-time"),
            change=lambda: stages.run_event(events_client, "ra214-delayed"),
            agents=agents,
        )
        async with engine.session() as session:
            await bench.run_agent_freshness(
                session, steps, recorder,
                offsets=[float(x) for x in args.offsets.split(",")], repeats=args.rounds, seed=args.seed,
            )  # fmt: skip
        if not args.external_only:
            # Tool and model calls per run, from the agent's system log (one snapshot query).
            name = sqlfiles.statement_name("check", "e5-log", str(int(time.time())))
            rows = agent_client.query(name, log_counts.statement).records()
            recorder.event("agent_log", rows=rows)


def run_failure_experiment(
    cfg: config.LabConfig, engine: ContextEngine, recorder, dataset, scenario: str
) -> None:
    """Wire E6 to the bench pipeline. Every scenario changes only bench_ objects,
    except `connection`, which deletes and recreates the agent's MCP connection."""
    import datetime as dt

    from streamhouse_lab import bench, datagen, failure
    from streamhouse_lab.kafka_io import BenchProducer

    table = bench.STATE_TOPIC
    probe = datagen.probe_flight(dataset)
    producer = BenchProducer.connect(cfg)
    counts_check = sqlfiles.parse(sqlfiles.stage("bench").path / "checks" / "bench-counts.sql")

    def lightning(topic: str, sql: str, rows: int = 1) -> list[list]:
        from streamhouse_lab.context import result_rows

        _, calls = engine.run([("queryData", query_arguments(topic, sql, rows))])
        return result_rows(calls[0].payload)

    def first(topic: str, sql: str) -> str | None:
        rows = lightning(topic, sql)
        return str(rows[0][0]) if rows else None

    def topic_status() -> dict:
        out: dict = {}
        scope = ["--environment", cfg.environment_id, "--cluster", cfg.kafka_cluster_id, "-o", "json"]
        commands = {f"lightning:{t}": ["confluent", "rtce", "rtce-topic", "describe", t, *scope]
                    for t in (bench.SOURCE_TOPIC, table) if t in cfg.lightning_topics}  # fmt: skip
        if bench.SOURCE_TOPIC in cfg.tableflow_topics:
            commands[f"tableflow:{bench.SOURCE_TOPIC}"] = [
                "confluent", "tableflow", "topic", "describe", bench.SOURCE_TOPIC, *scope,
            ]  # fmt: skip
        for label, argv in commands.items():
            proc = subprocess.run(argv, capture_output=True, text=True, check=False)
            try:
                body = json.loads(proc.stdout)
            except ValueError:
                body = {"raw": (proc.stdout or proc.stderr).strip()[:300]}
            out[label] = {
                k: v
                for k, v in body.items()
                if any(w in k.lower() for w in ("phase", "status", "error", "suspend"))
            }
        return out

    with FlinkClient(cfg) as client:

        def mt_status() -> dict:
            body = client.materialized_table(table)
            status, spec = body.get("status") or {}, body.get("spec") or {}
            return {k: status.get(k) for k in ("phase", "detail", "version")} | {
                "stopped": spec.get("stopped")
            }

        def counts() -> dict:
            name = sqlfiles.statement_name("check", "e6", str(int(time.time() * 1000)))
            return {
                str(r["risk"]): int(r["passengers"])
                for r in client.query(name, counts_check.statement).records()
            }

        def run_sql(sql: str) -> None:
            name = sqlfiles.statement_name("e6", str(int(time.time() * 1000)))
            client.submit(name, sql)
            client.wait(name, until=frozenset({"COMPLETED", "RUNNING"}))

        def write_marker(i: int) -> str:
            update = datagen.marker_update(probe, i)
            producer.send_one(update)
            return update.value["estimated_time"].strftime(bench.TIMESTAMP_TEXT)

        def write_new_passenger() -> str:
            arrive = datagen.SERVICE_DAY + dt.timedelta(hours=9)
            producer.send([
                datagen.flight_record("B-IN99990", "ORD", datagen.HUB, arrive),
                datagen.flight_record("B-OUT99990", datagen.HUB, "SEA", arrive + dt.timedelta(minutes=120)),
                datagen.Record("bench_passenger_itineraries", {"passenger_id": "B-P999990"},
                               {"inbound_flight_id": "B-IN99990", "connecting_flight_id": "B-OUT99990"}),
            ])  # fmt: skip
            return "B-P999990"

        def write_unreadable() -> None:
            record = datagen.flight_record("B-IN99998", "ORD", datagen.HUB, datagen.SERVICE_DAY)
            ack = producer.send_unreadable(
                record, b"not avro: written by a producer with the wrong serializer"
            )
            recorder.event("unreadable_written", partition=ack.partition, offset=ack.offset)

        world = failure.World(
            mt_status=mt_status,
            set_stopped=lambda stopped: client.set_materialized_table_stopped(table, stopped),
            probe_state=lambda: first(table, bench.probe_query()),
            probe_source=lambda: first(bench.SOURCE_TOPIC, bench.source_probe_query(probe.key["flight_id"])),
            write_marker=write_marker,
            counts=counts,
            lightning_sample=lambda risk: len(lightning(table, bench.risk_sample_query(risk), 5)),
            run_sql=run_sql,
            write_new_passenger=write_new_passenger,
            passenger_visible=lambda pid: bool(lightning(table, bench.passenger_lookup(pid, "PASSENGER_ID"))),
            write_unreadable=write_unreadable,
            topic_status=topic_status,
        )
        if scenario == "stop-resume":
            print(failure.stop_resume(world, recorder))
        elif scenario == "evolve":
            evolve_sql = sqlfiles.parse(config.SQL_DIR / "e6" / "evolve-bench-passenger-state.sql").statement
            print(failure.evolve(world, recorder, evolve_sql))
        elif scenario == "unreadable":
            print(failure.unreadable_record(world, recorder))
        elif scenario == "observe":
            print(failure.observe(world, recorder, "observe", topics=topic_status(),
                                  probe_source=world.probe_source(), counts=counts()))  # fmt: skip
        elif scenario == "connection":
            print(connection_scenario(cfg, client, recorder))


def connection_scenario(cfg: config.LabConfig, client: FlinkClient, recorder, delay: float = 5.0) -> dict:
    """Delete the agent's MCP connection while a run is in flight, then recreate it and run again."""
    import concurrent.futures

    check = sqlfiles.parse(sqlfiles.stage("agent").path / "checks" / "rebooking-offers.sql")

    def run_agent(label: str) -> dict:
        name = sqlfiles.statement_name("check", "e6", label, str(int(time.time() * 1000)))
        started = time.perf_counter()
        try:
            rows = client.query(name, check.statement).records()
            outcome = {"rows": [{k: str(v)[:300] for k, v in r.items()} for r in rows]}
        except Exception as exc:  # the failure is the observation
            outcome = {"error": str(exc)[:500]}
        return {"elapsed_s": round(time.perf_counter() - started, 1), **outcome}

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run_agent, "during-delete")
        time.sleep(delay)
        deleted = agent_connection.delete(cfg)
        recorder.event("step", step="connection_deleted", deleted=deleted, after_s=delay)
        during = future.result()
    recorder.event("step", step="run_during_delete", **during)
    without = run_agent("without-connection")
    recorder.event("step", step="run_without_connection", **without)
    agent_connection.create(cfg)
    recorder.event("step", step="connection_recreated")
    after = run_agent("after-recreate")
    recorder.event("step", step="run_after_recreate", **after)
    result = {"during": during, "without": without, "after": after}
    recorder.event("result", scenario="connection", **result)
    return result


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


def cmd_chart(args: argparse.Namespace) -> int:
    """Offline: render recorded results as blog figures, or print the commit-clock fit."""
    from streamhouse_lab import charts

    files = [Path(f) for f in args.files]
    expected = {"freshness-runs": 2, "stages": 1, "commit-clock": 1}[args.kind]
    if len(files) != expected:
        print(f"chart {args.kind} needs {expected} results file(s)", file=sys.stderr)
        return 2
    out = Path(args.out)
    if args.kind == "freshness-runs":
        fixed, jitter = (charts.freshness_values(f) for f in files)
        written = charts.write_figures(
            "freshness-runs", lambda lang: charts.render_freshness_runs(fixed, jitter, lang), out
        )
    elif args.kind == "stages":
        samples = charts.stages(files[0])
        written = charts.write_figures(
            "freshness-stages", lambda lang: charts.render_stages(samples, lang), out
        )
    else:
        clock = charts.commit_clock(charts.stages(files[0]))
        print(
            f"{clock.samples} commits over {clock.intervals} intervals: one every {clock.period:.2f} s, "
            f"every observation within {clock.max_residual:.2f} s of that clock"
        )
        return 0
    for path in written:
        print(path)
    return 0


def cmd_metrics(args: argparse.Namespace) -> int:
    """Read-only: Lightning scanned bytes and CCU-minutes per topic for an interval (UTC, ISO 8601)."""
    from streamhouse_lab import metrics

    usage = metrics.usage(config.load(), args.start, args.end)
    print(f"{'topic':32} {'scanned bytes':>16} {'CCU-minutes':>12}")
    for topic, values in sorted(usage.items()):
        print(f"{topic:32} {values.get('scanned_bytes', 0):>16,.0f} {values.get('ccu_minutes', 0):>12,.2f}")
    return 0


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
    p.add_argument("action", choices=["gen", "e1", "e2", "e3", "e4", "e5", "e6", "hybrid", "summarize"])
    p.add_argument(
        "--scenario",
        choices=["stop-resume", "evolve", "unreadable", "connection", "observe"],
        help="e6: which failure or change to run",
    )
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
    p.add_argument(
        "--read",
        choices=["state", "source"],
        default="state",
        help="e1: poll the derived bench_passenger_state, or bench_flight_status itself",
    )
    p.add_argument("--offsets", default="0,5,15,30", help="e5: seconds between the change and the agents")
    p.add_argument("--rounds", type=int, default=5, help="e5: cycles per offset")
    p.add_argument("--external-only", action="store_true", help="e5: run only the external client")
    p.add_argument(
        "--external",
        choices=["claude-code", "none"],
        default="claude-code",
        help="e5: also run the task in an external MCP client",
    )
    p.set_defaults(func=cmd_bench)

    p = sub.add_parser("chart", help="Phase 4: render recorded results as blog figures (offline)")
    p.add_argument("kind", choices=["freshness-runs", "stages", "commit-clock"])
    p.add_argument("files", nargs="+", help="freshness-runs: fixed-gap E1 file, then jittered E1 file")
    p.add_argument("--out", default=str(config.REPO_ROOT / "pages" / "_includes" / "charts"))
    p.set_defaults(func=cmd_chart)

    p = sub.add_parser("metrics", help="Lightning scanned bytes and CCU-minutes per topic (read-only)")
    p.add_argument("start", help="UTC start, e.g. 2026-11-15T15:00:00Z")
    p.add_argument("end", help="UTC end")
    p.set_defaults(func=cmd_metrics)

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
