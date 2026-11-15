# confluent-streamhouse-lab

A reproducible lab for the Confluent Cloud Streamhouse (Kafka, Flink SQL, materialized tables, Lightning Tables, Tableflow, the Real-Time Context Engine) built on the River Air scenario from Confluent's quick start.
Vendor-neutral and open source Streamhouse experiments belong in the separate `streamhouse-lab` repository, not here.

## Safety rules (non-negotiable)

- Never print, log, or commit an API key secret, password, token, or Terraform state.
  Wrap credentials in `redact.Secret` / `redact.ApiKey` the moment they are read.
- Never run `terraform apply`, `terraform destroy`, `scripts/deploy`, `scripts/cleanup`, `scripts/verify-live`, or any Confluent CLI command that creates, changes, or deletes cloud resources without the owner's explicit approval for that specific action.
  `scripts/deploy --plan`, `scripts/cleanup --verify`, `uv run lab doctor`, and read-only `confluent ... list/describe` are fine.
- Every live test and every `uv run lab apply|check|event|query|context|iceberg|bench` call is billed, except `lab bench gen --dry-run` and `lab bench summarize`. Ask first.
- Do not touch the organization's other environments.

## Layout

- `terraform/` - platform (`main.tf`), Lightning Tables and the context reader (`lightning.tf`), Tableflow (`tableflow.tf`). Local state, gitignored, mode 600.
- `sql/<NN>-<stage>/` - one statement per file, applied in name order by `uv run lab apply <stage>`; `checks/` holds snapshot queries; `sql/events/` holds replayable business events.
- `src/streamhouse_lab/` - `uv run lab` CLI: Flink statements API client, stage runner, RTCE MCP client, Tableflow Iceberg reader, agent connection, and Phase 4 tooling (`datagen`, `kafka_io`, `measure`, `bench`, `summarize`).
- `tests/unit/` - offline, run by default. `tests/live/` - marked `live`, billed, ordered by capability.
- `docs/` - discovery, architecture, plan, one page per capability, Phase 4 methodology.

## Conventions

- Terraform owns infrastructure; SQL files own Flink objects; the CLI is used only where neither works (the agent's MCP connection).
- SQL files never contain environment IDs or the catalog name; the runner sets `sql.current-catalog` and `sql.current-database`.
- A header comment `-- lab:mode=snapshot` makes a file a snapshot query. Adapted files say where they came from; see `NOTICE`.
- Statement names: `lab-<stage>-<file>` for applied files (skipped if already healthy), `check-`, `event-`, `adhoc-` with a timestamp for repeatable ones.
- Capability docs record "expected" from official sources and "actual" only from real runs with date and command. Never invent results or benchmark numbers.
- Experiments write only to the `bench_` tables, never to the quick start tables. Raw results stay in `results/` (gitignored); summaries with methodology go in `docs/experiments/`.
- Verify commands and API shapes against current Confluent docs, CLI `--help`, or provider docs before relying on them. Record the source in `docs/discovery.md`.

## Commands

```sh
scripts/verify-local          # fmt, validate, ruff, shellcheck, unit tests (offline)
uv run pytest                 # unit tests only
uv run lab doctor             # local prerequisites
scripts/deploy --plan         # see what would change, never applies
```
