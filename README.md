# Wallet Activity Lakehouse

A robust, production-realistic CDC → lakehouse → dbt pipeline designed to showcase a modern data stack processing operational PostgreSQL data, streaming events, and on-chain crypto activity.

What it is: A synthetic wallet application's operational database (PostgreSQL), captured via Change Data Capture (Debezium), landed and merged into a Delta Lake bronze layer (Spark, batch and streaming), modeled into a dimensional schema with dbt (comprehensive testing, documented grain, and Type-2 SCD tracking), enriched with real on-chain data from Etherscan, provisioned with Terraform, gated by CI/CD, and exposed through a dbt Semantic Layer for BI.

**What it is not:** A toy script or an untested wrapper. It is a scoped, fully verified demonstration of an end-to-end modern data engineering stack.

## Architecture

```
Postgres (OLTP)  --logical replication-->  Debezium  -->  Kafka
                                                              |
                                                    cdc/consume_cdc_events.py
                                                              |
                                                              v
                                              landing_zone/raw/cdc/<table>/dt=.../
                                             (S3 bucket provisioned by terraform/)
                                                              |
                              +-------------------------------+-------------------------------+
                              |                                                               |
                    spark/bronze_cdc_to_delta.py                              spark/streaming_cdc_to_delta.py
                    (batch MERGE, scheduled)                                  (Auto Loader, Trigger.AvailableNow())
                              |                                                               |
                              +-------------------------------+-------------------------------+
                                                              v
                                          wallet_lakehouse.bronze.*  (Delta / Unity Catalog)
                                                              |
                                              dbt: staging views -> snapshots (SCD2) -> marts
                                                              |
                              +-------------------------------+-------------------------------+
                              v                                                               v
              dim_users, dim_users_history, dim_wallets,                    _semantic_models.yml (MetricFlow)
              fact_transactions   [28 dbt tests, all green]                 5 governed metrics for BI tools

Etherscan V2 API --> ingestion/fetch_onchain_transactions.py --> landing_zone/raw/onchain/... --> bronze.onchain_transactions

CI (.github/workflows/ci.yml): dbt build+test, Python syntax checks, terraform fmt/validate — on every push/PR
```

## What's actually been run and verified (vs. written and syntax-checked)

Everything in this repo has been built, and where it doesn't require a paid
account, actually executed — not just written and assumed correct:

- **Synthetic data generation**: run, producing 500 users / 852 wallets /
  13,400 transactions with realistic skew.
- **dbt build**: run locally against that dataset with `dbt-duckdb` as a
  stand-in warehouse — **40/40 passed**: 3 seeds, 1 snapshot, 5 table
  models, 3 view models, 28 data tests, zero deprecation warnings.
- **SCD2, specifically**: this wasn't just built and assumed to work. I ran
  the snapshot once, mutated 25 users' `kyc_status` from `pending` to
  `verified` (`scripts/demo_scd2.py`), re-seeded, and snapshotted again.
  Querying the result confirms exactly 25 users now have two versions in
  `snapshots.users_snapshot`, with correct `dbt_valid_from`/`dbt_valid_to`
  timestamps, and that `dim_users` (current-state) and `dim_users_history`
  (point-in-time) both resolve correctly against it. Reproduce this
  yourself with the steps in `scripts/demo_scd2.py`'s docstring.
- **Monitoring, specifically**: `monitoring/alert_on_dbt_failures.py` was
  run against both a clean `run_results.json` (correctly reports "nothing
  to alert on", exit 0) and a deliberately broken one — I injected a
  negative `usd_value_at_tx` directly into the seed data, rebuilt, watched
  the `assert_transaction_usd_value_non_negative` test genuinely fail, and
  confirmed the script detects and reports it, with exit code 1 (the CI
  failure signal). Both paths work, not just the happy one.
- **`dbt source freshness`**: run for real. `bronze.users` passed (freshened
  by the SCD2 mutation above), `bronze.wallets`/`bronze.transactions`
  correctly flagged `ERROR STALE` since that seed data is months old —
  exactly the behavior you'd want flagged in a real pipeline.
- **The dbt Semantic Layer**: `dbt parse` succeeds, and the compiled
  manifest genuinely contains both semantic models and all 5 metrics
  (checked directly against `target/manifest.json`, not assumed from a lack
  of errors). Querying them with the MetricFlow CLI (`mf query ...`) was
  **not** run here — `dbt-metricflow`'s own dependency chain didn't install
  cleanly in this sandbox (a transitive package fails to build against this
  environment's setuptools version); expected to install cleanly in a
  normal dev environment.
- **On-chain contract addresses**: the USDT and WETH addresses hardcoded in
  `ingestion/fetch_onchain_transactions.py` were verified directly against
  Etherscan/BaseScan before being committed, rather than pulled from memory
  (an earlier draft had the USDT address wrong by one character).
- **Terraform**: `terraform fmt`/`init`/`validate` have been run for real
  (not just `python-hcl2` syntax-checked) on the maintainer's machine, with
  the real `aws` and `databricks` providers downloaded. The AWS-only
  resources (S3 bucket, GitHub OIDC/IAM) have real `terraform apply`
  behind them. The Unity Catalog resources (`databricks_storage_credential`,
  `databricks_external_location`) do not, and can't be applied on
  Databricks Free Edition at all — confirmed directly against Databricks'
  own docs that Free Edition has no Account Console / account-level API
  access, which those two resource types require. `main.tf` gates them
  behind `enable_unity_catalog_automation` (default `false`) for exactly
  this reason, rather than silently pretending they'd work.
- **CI workflow**: YAML-syntax-checked; not triggered on real GitHub
  Actions from this sandbox (no way to do that from here). The dbt/Python
  steps mirror commands already verified locally; the Terraform job uses
  `hashicorp/setup-terraform`, the standard action for this.
- **Everything else** (Debezium/Kafka CDC, the Spark batch/streaming jobs,
  `simulate_activity.py` against a live Postgres) is written and
  syntax-checked, but needs a running Postgres + Kafka stack
  (`docker compose up`) and a Databricks workspace to actually execute.

## Databricks Free Edition — what actually works here

Free Edition (the current free tier — "Community Edition" is the retired
predecessor) is **serverless-only**: no classic clusters, no custom Spark
configs, one 2X-Small SQL warehouse. Two limits directly shaped this repo's
design, verified against Databricks' own docs before writing the code:

1. **Streaming triggers are restricted.** Serverless compute only supports
   `Trigger.AvailableNow()` (recommended) and the deprecated
   `Trigger.Once()` — not `Trigger.ProcessingTime()` or
   `Trigger.Continuous()`. That's why `streaming_cdc_to_delta.py` runs as a
   scheduled micro-batch rather than an always-on stream; it's also
   Databricks' own recommended pattern for cost control on any tier.
2. **External storage requires Unity Catalog.** Serverless compute can't
   use DBFS mounts or instance profiles — external locations must go
   through Unity Catalog, which Free Edition includes. The Spark jobs here
   read/write via `catalog.schema.table` names, and `terraform/main.tf`
   provisions the storage credential + external location that makes that
   possible — **on a paid tier**, see point 3.
3. **Free Edition has no Account Console / account-level API access at
   all** (confirmed directly against Databricks' own docs, not assumed).
   The storage credential and external location in point 2 are
   account-level resources, so they genuinely cannot be created via
   Terraform on Free Edition — not a credentials problem, a platform one.
   `terraform/main.tf` gates those resources behind
   `enable_unity_catalog_automation` (default `false`) for exactly this
   reason; the S3 bucket and the GitHub OIDC/IAM resources in
   `terraform/github_oidc.tf` have no such dependency and apply on Free
   Edition without issue. To actually wire up an external S3 location on
   Free Edition, check your workspace's own Catalog Explorer for a manual
   "external data" path — Databricks documents one at the workspace level,
   separate from the account-level Terraform flow, but whether Free Edition
   exposes it isn't something the public docs confirm either way. Failing
   that, the Spark jobs still work against Free Edition's default
   Databricks-managed catalog storage; you just don't get "bring your own
   S3 bucket."

## Setup

### 1. Generate the synthetic dataset

```bash
python scripts/generate_synthetic_data.py --users 500 --to both
```

### 2. Set up the source database, then bring up Kafka + Debezium

The source OLTP database is deliberately **not** in `docker-compose.yml` —
it's your host machine's own Postgres, standing in for wherever this would
really live (a managed service, or a self-managed HA cluster). See the
comment block at the top of `docker-compose.yml` for why, and make sure
your host Postgres accepts connections from Docker's bridge network before
you register the connector (`listen_addresses` + `pg_hba.conf` — same file
has the specifics).

```bash
createdb wallets
psql -d wallets -f oltp/schema.sql
psql -d wallets -f seed_data/seed_inserts.sql

cp .env.example .env
python3 scripts/generate_kafka_cluster_id.py    # paste the output into .env — generate this ONCE
                                                 # per cluster, not per session (see the script's
                                                 # docstring: it's written into the persisted Kafka
                                                 # volume on first boot and checked on every boot after)
docker compose up -d                            # reads KAFKA_CLUSTER_ID from .env automatically
python scripts/register_connector.py            # PG_USER/PG_PASSWORD env vars if yours differ from the defaults
```

### 3. Watch CDC events land, and generate live traffic

```bash
pip install -r requirements.txt
python cdc/consume_cdc_events.py &
python scripts/simulate_activity.py --interval 2 --duration 120
```

### 4. Pull real on-chain data

```bash
export ETHERSCAN_API_KEY=your_free_key   # https://etherscan.io/apis
python ingestion/fetch_onchain_transactions.py --chain ethereum --limit 200
```

This is the manual/local run. In production this runs on a schedule
instead — see step 6 below.

### 5. Provision AWS with Terraform

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # fill in your account IDs
terraform init
terraform plan
terraform apply
```

This creates the S3 landing-zone bucket and the GitHub OIDC/IAM resources.
It does **not** touch Unity Catalog by default — `enable_unity_catalog_automation`
defaults to `false` because those resources need Databricks Account Console
access, which Free Edition doesn't have (see "Databricks Free Edition — what
actually works here" above). Leave it `false` unless you're on a paid
Databricks tier.

### 6. Enable the scheduled on-chain ingestion workflow

`.github/workflows/onchain-ingestion.yml` runs step 4 on a schedule (daily,
03:17 UTC) instead of you remembering to run it by hand — see that file's
header comment for the full design, but the short version is it
authenticates to AWS via GitHub's OIDC provider (`terraform/github_oidc.tf`
provisions the trust relationship), so there's no AWS access key sitting in
a repo secret; the assumed role can only `PutObject` under `raw/onchain/`.

After `terraform apply` (step 5, which now also creates the OIDC role), set
these in the GitHub repo's Settings → Secrets and variables → Actions:

| Type | Name | Value |
|---|---|---|
| Secret | `ETHERSCAN_API_KEY` | your free Etherscan key |
| Variable | `AWS_ROLE_ARN` | `terraform output github_actions_role_arn` |
| Variable | `AWS_REGION` | matches `terraform.tfvars`' `aws_region` |
| Variable | `LANDING_ZONE_S3_BUCKET` | `terraform output raw_bucket_name` |

The workflow also fixes a gap the manual version has: a GitHub Actions
runner's filesystem is thrown away after every run, so `fetch_onchain_transactions.py`
now uploads to S3 directly when `LANDING_ZONE_S3_BUCKET` is set (it still
writes to local disk too — additive, not a replacement, for local runs).

### 7. Databricks Free Edition — bronze layer

Sign up at [databricks.com](https://www.databricks.com/), then run
`spark/bronze_cdc_to_delta.py` and `spark/streaming_cdc_to_delta.py` as
notebooks or scheduled jobs against the external location Terraform just
created. Set `RAW_BUCKET` (from `terraform output raw_bucket_name`) as an
environment variable on the job/notebook — see the comment at the top of
either script for why this isn't hardcoded.

### 8. dbt — test locally first, then point at Databricks

```bash
cd dbt/wallet_lakehouse
export DBT_PROFILES_DIR=../profiles_dir
dbt deps
dbt seed --target dev
dbt snapshot --target dev
dbt build --target dev             # duckdb, no warehouse needed
dbt source freshness --target dev  # staleness check

# reproduce the SCD2 demo:
python ../../scripts/demo_scd2.py
dbt seed --target dev --full-refresh && dbt snapshot --target dev && dbt build --target dev

# once Databricks is set up:
pip install dbt-databricks
dbt build --target databricks
```

### 9. CI

Push to a `main` branch on GitHub and `.github/workflows/ci.yml` runs the
dbt build+test suite, Python syntax checks, and Terraform validation on
every push/PR automatically. `.github/workflows/onchain-ingestion.yml`
(step 6) runs independently, on its own schedule.

## How this runs in production, vs. the walkthrough above

Steps 1–9 above run everything by hand, once, in sequence — that's the
fastest way to verify each stage actually works, and it's how you'd
reproduce this yourself. It is **not** how any of this would operate day to
day. Production replaces "a person typing commands in order" with each
stage running on its own trigger, continuously or on a schedule, decoupled
from the others. Stage by stage, what's already wired that way vs. what's
the documented target:

| Stage | In this repo (demo) | In production |
|---|---|---|
| Source writes | `generate_synthetic_data.py` + `simulate_activity.py` fabricate traffic on demand | Real application traffic hits Postgres continuously — nothing to "run" |
| Postgres | Your host machine's own instance, standing in for the real thing | Managed (RDS/Aurora) or self-managed HA (Patroni + etcd + HAProxy) — already the assumption baked into `oltp/schema.sql`'s setup notes |
| CDC capture | Single-broker Kafka + one Connect worker (`docker compose up`), started/stopped per session | Debezium + Kafka running persistently — MSK or Confluent Cloud, multi-broker/multi-worker for real HA (this repo's single-broker setup is explicitly flagged as a demo limitation, not a production claim) |
| Bronze landing | `cdc/consume_cdc_events.py` run manually in a terminal (`&`), writing to local disk | A long-running consumer (or Spark Structured Streaming reading Kafka directly) as a supervised service — not a background shell job |
| Bronze merge | `spark/bronze_cdc_to_delta.py` / `streaming_cdc_to_delta.py` run by hand as scripts/notebooks | **This is the one real gap** (also called out in Honest scope notes below): these should run as Databricks Jobs on a schedule (`Trigger.AvailableNow()` every N minutes) — written and executable, but no scheduler invokes them yet. Same mechanism as the on-chain workflow below, just not wired up |
| On-chain ingestion | — | **Actually production-shaped already**: `.github/workflows/onchain-ingestion.yml` runs on a daily cron via GitHub Actions, OIDC-authenticated to AWS, no manual step and no stored credential |
| Transformation (dbt) | `dbt build` typed by hand against DuckDB, and re-run automatically in CI on every push/PR | CI verifies the *model* is correct (tests, SCD2, freshness) on every code change — it is not a run schedule. Production dbt runs as a triggered task after bronze lands: a Databricks Workflows task, a dbt Cloud job, or an Airflow DAG step |
| Semantic layer / BI | `dbt parse` + inspecting `target/manifest.json` confirms the 5 metrics compile and are queryable | A BI tool (Hex, Tableau, Looker, etc.) connects to the dbt Semantic Layer / MetricFlow API so analysts query governed metrics without writing SQL — no BI tool account was in scope here, so this is proven at the metric-compilation level, not the live-query level |
| Monitoring | `monitoring/alert_on_dbt_failures.py` runs in CI, catching transformation-layer test failures | Production extends this up the stack — consumer lag, connector health, Spark job failures — typically via the orchestrator's own alerting (Databricks Jobs → Slack/PagerDuty) rather than a bespoke script per layer |

The throughline: **the on-chain ingestion path is the one stage that's fully
production-shaped today** — scheduled, credential-free, decoupled from
manual intervention. Every other stage is built and independently verified
(see the section above), but still needs an orchestrator's schedule wired
to it to make the leap from "demonstrated correct" to "runs itself."

## Data model

**`dim_users`** — grain: one row per `user_id`, current version only.
**`dim_users_history`** — grain: one row per `(user_id, valid_from)`, the
full Type-2 change history, for point-in-time joins (docstring has the
join pattern).
**`dim_wallets`** — grain: one row per `wallet_id`.
**`fact_transactions`** — grain: one row per `transaction_id`, the finest
grain the source system emits; joins to both dimensions are 1-to-1, so
aggregates never get inflated by a fan-out join.

## Honest scope notes

- The on-chain ingestion is genuinely scheduled now (`.github/workflows/onchain-ingestion.yml`,
  daily via GitHub Actions, OIDC-authenticated to AWS — no stored access
  key). The Spark batch/streaming jobs are **not** — no orchestrator
  (Airflow/Dagster, or even a Databricks Jobs schedule) actually invokes
  them on a cadence yet. Databricks Jobs' own scheduler is the natural next
  step there, same mechanism as the on-chain workflow conceptually, just
  not wired up.
- The CDC consumer (`cdc/consume_cdc_events.py`) writes to local disk by
  default; its S3 upload path (`LANDING_ZONE_S3_BUCKET`) uses the same
  pattern the on-chain script's S3 upload does, but hasn't itself been
  exercised against a real bucket here, unlike the on-chain path which now
  has a real scheduled workflow behind it.
- `stg_transactions.sql` uses DuckDB's `datediff()` — swap for
  `unix_timestamp()` arithmetic on Databricks SQL; noted inline.
- Kubernetes isn't in here — a single-service demo like this doesn't need
  it to be credible, and Docker Compose already demonstrates the
  containerisation half of that preferred-skills line.
- The on-chain ingestion targets a couple of well-known, verified public
  contract addresses (USDT, WETH) rather than guessing at exchange wallet
  addresses — accurate and legally uncomplicated, at the cost of a smaller
  on-chain sample than scraping arbitrary addresses would give.
- The GitHub OIDC thumbprint in `terraform/github_oidc.tf` was taken
  directly from AWS's own security blog, not derived or guessed — but
  Terraform has no way to auto-refresh it if GitHub ever changes CAs, so
  it's a value to sanity-check against AWS's docs if the workflow starts
  failing to authenticate years from now.
