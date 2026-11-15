# Implementation plan

The lab grows one capability at a time.
Each step lists what it creates, what keeps billing after it, and how it is verified.
Steps marked **approval** create or change billable resources and wait for an explicit yes.

## Step 0: prerequisites (no cloud cost)

1. **approval (creates a credential):** create a Cloud API key for Terraform with your user account, and put it in `.env`:
   ```sh
   confluent api-key create --resource cloud --description "confluent-streamhouse-lab terraform"
   cp .env.example .env   # paste the key and secret
   direnv allow
   ```
   A Cloud API key can manage the whole organization, so it lives only in `.env`, which `park` backs up to 1Password and git ignores.
2. Run `uv run lab doctor` and `scripts/verify-local`.
3. Run `scripts/deploy --plan` to see the exact resources before approving step 1.

## Step 1: platform and capabilities 1 to 3

**approval:** `scripts/deploy` with an empty `terraform.tfvars`.

Creates: environment `river-air-lab` (Stream Governance Essentials), Basic cluster `river-air` in `us-east-1`, Flink compute pool with `max_cfu = 5`, service account `river-air-lab-operator` with EnvironmentAdmin on that environment, and its Flink API key.

Then, with the Flink statements API:

```sh
uv run lab apply sources            # capability 1: three tables, seed events
uv run lab check sources
uv run lab check transform          # capability 2: the join as a bounded query
uv run lab apply passenger-state    # capability 3: the materialized table
uv run lab check passenger-state    # expect OK, OK, NO_CONNECTION
uv run lab event ra417-delayed
uv run lab check passenger-state    # expect AT_RISK, AT_RISK, NO_CONNECTION
scripts/verify-live "capability_1 or capability_2 or capability_3"
```

Keeps billing: the `passenger_state` statement, at least 1 CFU while it runs.
Everything else is idle at no cost.

## Step 2: capability 4, Lightning Tables

**approval:** set `lightning_topics` in `terraform/terraform.tfvars` (see the example file) and run `scripts/deploy`.

Creates: Lightning Tables on `passenger_state`, `flight_status`, `hotel_inventory`; service account `river-air-lab-context-reader` with DeveloperRead on schema subjects and, on Basic, CloudClusterAdmin on the cluster (DeveloperRead on topics on Standard); and its Global API key.

Verify:

```sh
uv run lab context topics
uv run lab context query passenger_state "SELECT * FROM passenger_state WHERE risk = 'AT_RISK'"
scripts/verify-live capability_4
```

Keeps billing: Lightning storage for three small tables, and compute while data arrives or queries run.

## Step 3: capability 5, Tableflow

**approval:** `uv run lab apply history`, then add `tableflow_topics = ["flight_history"]` to `terraform.tfvars` and run `scripts/deploy`.

Creates: the `flight_history` table and six rows, the operator's Tableflow API key, and an Iceberg table in Confluent Managed Storage.

Verify: `uv run lab check history` (Flink), `uv run lab iceberg flight_history` (Iceberg alone), `scripts/verify-live capability_5` (both).

Keeps billing: one Tableflow topic-hour per hour, plus managed storage.

## Step 4: capability 6, the rebooking agent

**approval:** each agent run calls the managed model.

```sh
uv run lab agent-connection create   # CLI: Flink MCP connection with the context reader's key
uv run lab apply agent               # model, tool, agent, rebooking_requests
uv run lab check agent               # runs the agent once per at-risk passenger
scripts/verify-live capability_6
```

Also, optionally, connect Claude Code to the same engine to compare an external agent with the in-Flink agent (see [capability 6](capabilities/06-agent-mcp.md)).

## Step 5: Phase 4 experiments

Each experiment in [experiments](experiments/README.md) gets its own approval with its expected duration and resources before it runs.

## Cleanup

```sh
scripts/cleanup            # plan the destroy, type "destroy", then verify
scripts/cleanup --verify   # anytime: proves nothing named river-air-lab* is left
```

Manual: delete the Terraform Cloud API key when the lab is retired for good.

## Approval checklist for step 1

When you approve step 1, these run, in this order:

1. `scripts/deploy` (you type `apply` after reading the plan).
2. `uv run lab apply sources`, `uv run lab apply passenger-state`, the checks, and the `ra417-delayed` event.
3. `scripts/verify-live` for capabilities 1 to 3.

Nothing in step 2 or later runs without a separate approval.
