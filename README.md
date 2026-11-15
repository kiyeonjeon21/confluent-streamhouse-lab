# confluent-streamhouse-lab

A reproducible, test-driven lab for the Confluent Cloud Streamhouse.
It rebuilds Confluent's River Air quick start with Terraform and version-controlled Flink SQL, verifies each capability with automated tests, and then measures freshness, latency, throughput, agent behavior, failure recovery, and cost.

Vendor-neutral and open source Streamhouse experiments live in a separate `streamhouse-lab` repository.

## What it covers

| # | Capability | Confluent feature |
|---|---|---|
| 1 | Source tables and sample events | Kafka, Schema Registry, Flink `CREATE TABLE` |
| 2 | Transformations and joins | Flink SQL, snapshot queries |
| 3 | Continuously maintained passenger state | Flink materialized tables |
| 4 | Low-latency current-state reads | Lightning Tables |
| 5 | History as Iceberg | Tableflow, Confluent Managed Storage |
| 6 | AI agent with live business context | Streaming Agents, managed models, Real-Time Context Engine (MCP) |

How the pieces fit: [docs/architecture.md](docs/architecture.md).
What was verified before building, including limits and prices: [docs/discovery.md](docs/discovery.md).

## Status

- Phase 1 (discovery) and Phase 2 (repository) are done.
- Phase 3 is done: all six capabilities passed on 2026-11-15 in us-east-1 on a Basic cluster; the full live suite runs in about 3 minutes.
- Phase 4 (evaluation) is designed in [docs/experiments](docs/experiments/README.md) and not started.
- [docs/plan.md](docs/plan.md) lists each billable step and what it creates.
- Capability results are recorded in [docs/capabilities](docs/capabilities/README.md) as they are run.

## Prerequisites

- Confluent Cloud account with a payment method, logged in with `confluent login`.
- Terraform 1.9+, uv, the Confluent CLI, jq, direnv; shellcheck for `scripts/verify-local`.
- A Cloud API key for Terraform in `.env` (see [.env.example](.env.example) and [plan step 0](docs/plan.md#step-0-prerequisites-no-cloud-cost)).

## Usage

```sh
uv sync
scripts/verify-local                 # offline: terraform validate, lint, unit tests
uv run lab doctor                    # tools, logins, secret hygiene

scripts/deploy --plan                # what would be created
scripts/deploy                       # billable; asks you to type "apply"

uv run lab apply sources
uv run lab apply passenger-state
uv run lab event ra417-delayed
uv run lab check passenger-state
scripts/verify-live                  # billable end-to-end tests

scripts/cleanup                      # asks you to type "destroy", then verifies nothing is left
```

`uv run lab --help` lists every command.

## Repository layout

```
terraform/   environment, Basic cluster, Flink pool, service accounts, Lightning Tables, Tableflow
sql/         one Flink statement per file, grouped by stage; checks/ and events/
src/         the `lab` CLI: Flink statements API, stage runner, MCP client
tests/       unit (offline, default) and live (billed, marked)
scripts/     deploy, verify-local, verify-live, cleanup
docs/        discovery, architecture, plan, capabilities, experiments
```

## Secrets

- The only credential you handle is the Terraform Cloud API key in `.env`, loaded by direnv.
- Terraform creates every other key for a service account and keeps it in local state (gitignored, mode 600).
- `uv run lab` reads keys from `terraform output` into memory and masks them in all output.
- `tests/unit/test_repo_hygiene.py` fails if secret paths stop being ignored or a secret-looking string appears in the tree.

## License and attribution

The scenario and SQL are adapted from Confluent's documentation and `confluentinc/quickstart-streamhouse` (Apache-2.0).
See [NOTICE](NOTICE).
