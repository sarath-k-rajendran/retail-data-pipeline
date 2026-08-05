# Retail Medallion Data Pipeline (AWS Glue + Iceberg + CloudFormation)

A production-grade, Console-deployable Bronze -> Silver -> Gold retail data pipeline:

- **Compute**: AWS Glue ETL (Spark 3.x / Python), Glue Workflows for orchestration
- **Storage**: Amazon S3 data lake, Apache Iceberg table format
- **Catalog**: AWS Glue Data Catalog (as the Iceberg catalog)
- **IaC**: AWS CloudFormation only (no Terraform)
- **Deployment**: AWS Console only (no AWS CLI required anywhere in this runbook)

---

## 1. Architecture

```
                 ┌─────────────────────────────────────────────────────────────┐
                 │                     S3: raw/ (landing zone)                  │
                 │  product/ store/ promotion/ orders/ returns/                 │
                 │  web_sessions/ inventory_snapshot/  (each: yyyy/mm/dd/*.csv) │
                 └───────────────────────────┬─────────────────────────────────┘
                                              │  bronze_job.py
                                              ▼
                 ┌─────────────────────────────────────────────────────────────┐
                 │        Glue Catalog DB: retail_bronze  (Iceberg, append-only)│
                 │  product, store, promotion, orders, returns,                 │
                 │  web_sessions, inventory_snapshot                            │
                 │  + ingested_at, source_file, batch_id, load_id               │
                 └───────────────────────────┬─────────────────────────────────┘
                                              │  silver_job.py
                                              │  cleanse -> dedup -> validate/quarantine
                                              │  -> join dims -> MERGE upsert -> mask PII
                                              ▼
                 ┌─────────────────────────────────────────────────────────────┐
                 │        Glue Catalog DB: retail_silver (Iceberg, current-state)│
                 │  dim_product, dim_store, dim_calendar,                       │
                 │  fact_order_lines, fact_returns,                             │
                 │  fact_web_sessions, fact_inventory_snapshot                  │
                 │  + *_rejects quarantine tables (with reason codes)           │
                 └───────────────────────────┬─────────────────────────────────┘
                                              │  gold_job.py
                                              │  recompute affected date window (atomic
                                              │  overwritePartitions), full current-state
                                              ▼
                 ┌─────────────────────────────────────────────────────────────┐
                 │        Glue Catalog DB: retail_gold   (Iceberg, KPI marts)   │
                 │  kpi_sales_daily, kpi_returns_daily,                         │
                 │  kpi_inventory_daily, kpi_promo_lift,                        │
                 │  pipeline_run_log (observability audit trail)                │
                 └───────────────────────────┬─────────────────────────────────┘
                                              │
                                              ▼
                                     Amazon Athena / Glue
                                     query editor (BI-ready)
```

Orchestration: a single **AWS Glue Workflow** (`retail-mdp-dev-workflow`) chains the three
jobs with conditional triggers (`bronze SUCCEEDED -> silver`, `silver SUCCEEDED -> gold`).
You start it from the Glue Console; there is no CLI step anywhere in this runbook.

### 1.1 Assumptions

- **Region/env**: templates default to `us-east-1` / `Env=dev`; every resource name is
  `retail-mdp-<env>-<resource>`. Change the `Env` parameter (and region selector in Console)
  to deploy a second environment side by side.
- **No CDC columns required.** Source files carry business data only -- no `op_type` or
  `event_ts` metadata. Silver's upsert rule is simply: business key exists in target ->
  UPDATE, else -> INSERT (see [Simplified Incremental Logic](#43-incremental-logic)).
- **Reference data (product/store/promotion) is always shipped as a full snapshot**, in both
  `--load_mode DELTA` and `SNAPSHOT` -- these tables are small enough that resending them in
  full every batch is simpler and safer than diffing. Only orders/returns/web_sessions/
  inventory_snapshot are true incremental facts.
- **No explicit deletes from source.** The pipeline does not model hard deletes; a row that
  stops appearing in a source feed simply stays at its last-known state in Silver.
- **PII**: `customer_email` is the only PII field in scope. It is hashed (salted SHA-256) in
  Silver and dropped entirely after that -- the raw value exists only in Bronze, which has
  the same IAM/encryption boundary as everything else in this pipeline (no separate
  restricted-access tier is set up in this build; see [Security](#9-security--governance)
  for how to add one).
- **Single AWS account, single region** per environment. Cross-region/cross-account
  replication is out of scope.
- **Business keys** (drive Silver dedup + MERGE):

  | Table | Business key |
  |---|---|
  | orders (line grain) | `order_id + order_line_id` |
  | returns | `return_id` |
  | product | `product_id` |
  | store | `store_id` |
  | promotion | `promo_id` |
  | web_sessions | `session_id` |
  | inventory_snapshot | `store_id + product_id + snapshot_date` |

---

## 2. Repository layout

```
retail-data-pipeline/
├── plan.md                              Phased build plan (this repo was built incrementally against it)
├── README.md                            You are here
├── DEPLOYMENT.md                        Step-by-step deployment checklist for a real AWS account
├── requirements-dev.txt                 Local validation tooling: pytest, cfn-lint, pyflakes, pyyaml
├── .gitignore
├── scripts/
│   ├── build_common_zip.py              Packages jobs/common/ into dist/common.zip for --extra-py-files
│   └── stage_for_s3_upload.py           Reorganizes mock output into dist/raw_upload/ for one-shot S3 folder upload
├── infrastructure/cloudformation/
│   ├── 01-storage.yaml                  S3 buckets: raw, warehouse, scripts, temp, logs, athena-query-results
│   ├── 02-iam.yaml                      Glue execution role, PII salt secret, per-job log groups
│   ├── 03-catalog.yaml                  Glue Catalog databases: retail_bronze/silver/gold
│   ├── 04-glue-jobs.yaml                The 3 Glue Spark jobs (bronze/silver/gold) + Iceberg conf
│   ├── 05-workflow.yaml                 Glue Workflow + triggers (bronze -> silver -> gold)
│   └── 06-monitoring.yaml               SNS alarm topic, failure EventBridge rule, freshness SLA alarm
├── jobs/
│   ├── generate_mock_data.py            Local mock data generator (CLI)
│   ├── bronze_job.py                    Bronze Glue ETL job
│   ├── silver_job.py                    Silver Glue ETL job
│   ├── gold_job.py                      Gold Glue ETL job (KPI marts)
│   └── common/                          Shared library, zipped as common.zip for --extra-py-files
│       ├── schemas.py                   Single source of truth: Bronze DDL + Silver business keys
│       ├── iceberg_utils.py             Iceberg DDL/append/MERGE/overwrite-partitions helpers
│       ├── dq_utils.py                  Reusable DQ rule engine (completeness/validity/uniqueness/RI)
│       ├── logging_utils.py             Structured JSON run metrics + pipeline_run_log persistence
│       ├── workflow_utils.py            Glue Workflow run-properties handoff (batch_id/load_date)
│       ├── secrets_utils.py             Secrets Manager helper (PII salt)
│       └── dq_report.py                 Full-layer DQ report (callable inline from gold_job.py)
├── config/job_params/
│   ├── bronze_params.json               Reference Job Parameters for the Console
│   ├── silver_params.json
│   └── gold_params.json
├── tests/
│   ├── test_mock_data.py                pytest sanity tests for the mock generator (run locally)
│   └── data_quality_checks.py           Standalone post-hoc DQ validation (run in Glue/notebook)
├── data/mock/                            Generator output (gitignored)
└── dist/common.zip                      Build output of scripts/build_common_zip.py (gitignored)
```

---

## 3. CloudFormation design summary

Six independent stacks, deployed **in numeric order** (each imports prior stacks' Exports):

| # | Stack | Creates | Depends on |
|---|---|---|---|
| 1 | `01-storage.yaml` | 6 S3 buckets (raw, warehouse, scripts, temp, logs, athena-query-results), TLS-only bucket policies, SSE-S3, versioning/lifecycle per bucket | none |
| 2 | `02-iam.yaml` | Glue execution role (least-privilege, scoped to this project's buckets/databases only), PII salt secret (Secrets Manager, auto-generated), 3 per-job CloudWatch Log Groups | Stack 1 (bucket ARNs) |
| 3 | `03-catalog.yaml` | Glue databases `retail_bronze`/`retail_silver`/`retail_gold` | Stack 1 (warehouse bucket for `LocationUri`) |
| 4 | `04-glue-jobs.yaml` | 3 Glue Spark jobs (bronze/silver/gold), Iceberg + Glue Catalog Spark config, all job parameters | Stacks 1-3 |
| 5 | `05-workflow.yaml` | Glue Workflow + 3 triggers (1 starting ON_DEMAND trigger, 2 conditional success triggers) | Stack 4 |
| 6 | `06-monitoring.yaml` (optional) | SNS alarm topic (+ optional email subscription), EventBridge rule on job FAILED/TIMEOUT/ERROR/STOPPED, freshness SLA alarm (CloudWatch Logs metric filter heartbeat) | Stack 4 |

All resource names are deterministic: `retail-mdp-<Env>-<resource>` (S3 buckets additionally
suffix the AWS account ID for global uniqueness). Every stack takes `ProjectName` (default
`retail-mdp`) and `Env` (default `dev`) parameters -- **use the same values for both across
all 6 stacks**, or cross-stack `Fn::ImportValue` lookups will fail to resolve.

### 3.1 DPU budget for capacity-restricted accounts

`04-glue-jobs.yaml` creates exactly 3 job definitions (bronze/silver/gold), each using
**`G.1X`** (1 DPU/worker) with the AWS-enforced minimum of **2 workers** -- **2 DPU/job**.
That's the real floor for this job type: `G.025X` (0.25 DPU/worker) looks appealing for a
DPU-constrained account, but AWS Glue rejects it outright for `glueetl` (batch Spark ETL)
jobs -- it's only valid for `gluestreaming` jobs -- so it's not an option here regardless of
budget. Attempting it fails stack creation with:
```
G.025X is only supported for job command [gluestreaming].
```

| WorkerType | DPU/worker | Min DPU/job (2 workers) | Static sum, 3 jobs |
|---|---|---|---|
| G.1X (this stack's only viable choice) | 1.0 | 2.0 | **6.0** |
| G.025X | 0.25 | 0.5 | invalid for glueetl -- not usable |

If your account enforces a strict **sum of configured DPU capacity across all job
definitions**, 3 separate Spark ETL jobs at 2 DPU each (6.0 total) will not fit under a 4 DPU
cap -- there is no smaller valid `WorkerType`/`NumberOfWorkers` combination for `glueetl` to
shrink that further. In practice, many "total DPU" account restrictions are actually enforced
against **concurrent** usage (DPUs consumed by jobs running *at the same instant*), not a
static sum of every job definition's configured capacity -- and this pipeline's 3 jobs never
run concurrently: `05-workflow.yaml`'s triggers are chained on SUCCESS (bronze finishes
-> silver starts; silver finishes -> gold starts), so at most one job, using 2 DPU, is ever
actually running at once. If that's how your account's limit works, this deploys and runs
fine as-is.

**If stack creation or a job run is instead blocked by an explicit DPU-total enforcement**
(the same way `G.025X` was just rejected), the fix is to consolidate job *definitions*, not
shrink worker size further (there's no smaller valid size left): merge two of the three
layers into one job script that calls both stages' `main()` in sequence within a single Spark
session (e.g. silver+gold combined), bringing it to 2 job definitions x 2 DPU = 4 DPU exactly.
This is a real architecture change (fewer independently-retryable job definitions), so it's
done deliberately if/when you actually hit that wall, not preemptively.

### Why the Iceberg tables aren't in CloudFormation

`03-catalog.yaml` creates the **Glue databases** only. The Iceberg **tables** (schema,
partition spec, snapshots) are created by the Spark jobs themselves on first run, via
`CREATE TABLE IF NOT EXISTS ... USING iceberg LOCATION 's3://.../<table>/'` -- this is
standard practice for Iceberg-on-Glue: table metadata is managed by the table format, not by
IaC, and CloudFormation has no native `AWS::Glue::Table` support for Iceberg's metadata
model.

### Partitioning rationale (full detail in `plan.md` Phase 0)

Bronze/Silver fact tables partition on **business event date** (`days(<ts column>)`), not
ingestion date -- this keeps backfills/reprocessing of a past date range scoped to a small,
predictable partition set, and keeps the same date value meaningful across all three layers.
Small reference tables (product/store/promotion, dim_calendar) are **unpartitioned** --
partitioning a few-hundred-row table creates more metadata files than data files. Gold marts
partition by `days(kpi_date)` only; region/category/channel stay as regular columns pruned
via Iceberg column statistics rather than physical partitioning, to avoid exploding small
aggregate tables into a large number of tiny files.

---

## 4. Data flow in detail

### 4.1 Bronze

`bronze_job.py` reads each source's CSVs from `s3://<raw-bucket>/<source>/<yyyy>/<mm>/<dd>/*.csv`
and appends them, essentially as-is, into Iceberg tables in `retail_bronze`, adding
`ingested_at`, `source_file`, `batch_id`, `load_id`. No filtering, no cleansing, no
deduplication -- Bronze is raw-fidelity, append-only history.

`--csv_parse_mode` (`PERMISSIVE` | `DROPMALFORMED` | `FAILFAST`) controls how malformed CSV
rows are handled:
- **PERMISSIVE** (default): malformed rows are still written, with a `_corrupt_record`
  capture during parsing (then dropped from the final schema) and counted as
  `rows_rejected` in the run log -- full raw fidelity even for garbage input.
- **DROPMALFORMED**: malformed rows are silently skipped by Spark; the job estimates and
  logs how many were dropped.
- **FAILFAST**: the job aborts immediately on the first malformed row for that source.

**Idempotency**: before appending, the job checks whether `batch_id` was already committed
to that Bronze table. A plain rerun of the same `batch_id` is a no-op. Pass
`--force_reload true` to delete-and-replace that batch_id's rows (explicit backfill
correction).

### 4.2 Silver

`silver_job.py` processes the exact batch Bronze just landed (batch_id/load_date are handed
off automatically via the Glue Workflow's run-properties bag -- see
`common/workflow_utils.py`). For every source:

1. **Dedup**: within the incoming batch, keep one deterministic row per business key (the
   row with the greatest `(source_file, ingested_at)`).
2. **Validate/quarantine**: a rule-based DQ engine (`common/dq_utils.py`) flags null/blank
   required fields, out-of-range values, invalid enums, orphan foreign keys, and (for
   promotions) inverted date ranges. Failing rows are written to a `<table>_rejects`
   Iceberg table in `retail_silver` with a `reject_reasons` column (comma-joined reason
   codes) -- passing rows continue.
3. **Conform + enrich**: type/timezone standardization (UTC), joins against
   `dim_product`/`dim_store`/promotion attributes.
4. **Mask PII**: `customer_email` -> salted SHA-256 hash (`customer_email_hash`); the raw
   value is dropped.
5. **MERGE upsert**: `MERGE INTO <target> ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED
   THEN INSERT` on the business key -- naturally idempotent, so rerunning the same batch
   converges to the same end state rather than duplicating rows.

`dim_calendar` is generated (not sourced), populated once (2020-01-01 through 2035-12-31)
and is a no-op on subsequent runs.

### 4.3 Incremental logic

No CDC/`op_type` columns are required anywhere. The rule is uniform and simple:
**business_key exists in target -> UPDATE; else -> INSERT.** Both `--load_mode DELTA` and
`SNAPSHOT` follow this same rule -- `load_mode` is carried through purely for
logging/`load_id` naming, not branching logic. Reruns are made idempotent by
`batch_id`/`load_id` (Bronze's append guard) and by MERGE's inherent key-based semantics
(Silver).

### 4.4 Gold

`gold_job.py` **recomputes** (never incrementally patches) a bounded date window from
current-state Silver tables, using Iceberg's atomic `overwritePartitions` -- rerunning for
the same window with the same Silver contents always yields the same output, and
late-arriving facts (e.g. a return landing today against an order from last week) are
correctly folded in the next time that order's date falls inside the recompute window.

- **Recompute window**: trailing `--recompute_window_days` (default 3) ending at
  `load_date`. For a genuine historical correction, pass `--backfill_start_date` /
  `--backfill_end_date` explicitly instead.
- **`kpi_promo_lift`** is a different shape: lift is a comparison across many days, so it's
  computed as a point-in-time snapshot over a trailing `--promo_lift_window_days` (default
  90) history, tagged with `kpi_date = load_date`.

| Table | Grain | KPIs |
|---|---|---|
| `kpi_sales_daily` | day, region, category, channel | Gross Sales, Discount %, Net Sales, Gross Margin %, AOV, Units/Transaction |
| `kpi_returns_daily` | day (order/cohort date), region, category, channel | Return Rate % |
| `kpi_inventory_daily` | day, region, category, channel | Stockout Rate %, Inventory Turnover (daily proxy) |
| `kpi_promo_lift` | snapshot day, region, category, channel | Promo Lift % (trailing-window volume comparison) |

Every formula's exact definition and the reasoning behind grain choices (e.g. why return
rate is cohort-based on the *original* order date, not the return date) is documented inline
as code comments in `gold_job.py` -- read those before changing the numbers.

---

## 5. Console-only runbook

No AWS CLI is used anywhere below -- every step is a Console action. Do these **in order**.

> **Looking for the fast checklist version of these same steps (with copy-pasteable
> commands and checkboxes)?** See [`DEPLOYMENT.md`](DEPLOYMENT.md). It also covers the local
> virtual environment and the `common.zip` packaging step in more detail. This section is
> the narrative/reference version -- read it for the "why," use `DEPLOYMENT.md` for the
> "what do I click next."

### 5.1 Prerequisites

- An AWS account with Console access to CloudFormation, S3, Glue, IAM, Secrets Manager,
  CloudWatch, SNS, EventBridge, and Athena.
- Python 3.9+ locally, only to run the mock data generator (`jobs/generate_mock_data.py`) and
  the `common.zip` packaging script -- nothing in this runbook needs a local AWS SDK or CLI.
  See [`DEPLOYMENT.md`](DEPLOYMENT.md) §1 for the virtual environment setup.

### 5.2 Deploy the CloudFormation stacks

For each stack below: **CloudFormation Console > Stacks > Create stack > With new
resources > Upload a template file** -> select the file -> Next -> fill in parameters
(`ProjectName=retail-mdp`, `Env=dev`, keep other defaults unless you have a reason to
change them) -> Next -> check **"I acknowledge that AWS CloudFormation might create IAM
resources"** (needed for stacks 2 and, transitively, none of the others create IAM) ->
Create stack. Wait for `CREATE_COMPLETE` before starting the next one.

1. `infrastructure/cloudformation/01-storage.yaml` -> stack name `retail-mdp-dev-storage`
2. `infrastructure/cloudformation/02-iam.yaml` -> stack name `retail-mdp-dev-iam`
3. `infrastructure/cloudformation/03-catalog.yaml` -> stack name `retail-mdp-dev-catalog`
4. `infrastructure/cloudformation/04-glue-jobs.yaml` -> stack name `retail-mdp-dev-jobs`
   **-- see 5.3 below, you must upload the job scripts to S3 before this stack's jobs will
   actually run (the stack itself will create fine either way; only job *execution* needs
   the scripts in place).**
5. `infrastructure/cloudformation/05-workflow.yaml` -> stack name `retail-mdp-dev-workflow`
6. `infrastructure/cloudformation/06-monitoring.yaml` (optional) -> stack name
   `retail-mdp-dev-monitoring`. Set `AlarmEmail` if you want email alerts, then confirm the
   SNS subscription email that arrives after deploy.

### 5.3 Upload job scripts and shared library to S3

1. Go to **S3 Console > `retail-mdp-dev-scripts-<account-id>`**.
2. Click **Create folder**, name it `jobs`. Click into it, then **Upload > Add files** and
   select `jobs/bronze_job.py`, `jobs/silver_job.py`, `jobs/gold_job.py` (all 3 at once) from
   your local repo -> **Upload**.
3. Build the shared library archive: `python scripts/build_common_zip.py` (from an
   activated venv -- see [`DEPLOYMENT.md`](DEPLOYMENT.md) §1) writes `dist/common.zip` with
   `common/` as the zip root (i.e. the archive contains `common/__init__.py`,
   `common/iceberg_utils.py`, etc., not the files loose at the zip root) -- this matters
   because Glue's `--extra-py-files` only accepts individual `.py` files or a zip archive,
   not a bare S3 folder of loose files.
4. Back at the bucket root, click **Create folder**, name it `common`. Click into it, then
   **Upload > Add files** and select `dist/common.zip` -> **Upload**, so the final path is
   `s3://retail-mdp-dev-scripts-<account-id>/common/common.zip`.

This exact layout (`jobs/<job>.py`, `common/common.zip`) matches what
`04-glue-jobs.yaml`'s `ScriptLocation` and `--extra-py-files` already point at -- no other
configuration is needed for the jobs to find them.

### 5.4 Generate and upload mock data

Locally:
```
python3 jobs/generate_mock_data.py --out-dir data/mock/full --mode full --as-of-date 2026-08-03 --scale 1.0 --seed 42
```
This produces `product.csv`, `store.csv`, `promotion.csv`, `orders.csv`, `returns.csv`,
`web_sessions.csv`, `inventory_snapshot.csv` in `data/mock/full/`.

`bronze_job.py` reads each source at exactly `s3://<raw-bucket>/<source>/<yyyy>/<mm>/<dd>/*.csv`
-- **no extra `raw/` prefix inside the bucket** (the bucket's own name already says "raw").
The S3 Console's Upload dialog has no field to type an arbitrary destination key for loose
files, though, so build the nested folder structure locally first and upload it as one
folder-upload:

```bash
python3 scripts/stage_for_s3_upload.py --generated-dir data/mock/full --as-of-date 2026-08-03
```
This writes `dist/raw_upload/<source>/2026/08/03/<source>.csv` for all 7 sources. Then, in
**S3 Console > `retail-mdp-dev-raw-<account-id>`**, stay at the **bucket root** (don't
navigate into any prefix), click **Upload**, then **Add folder** (or drag-and-drop from
Finder/Explorer) and select all 7 folders inside `dist/raw_upload/` at once (`product`,
`store`, `promotion`, `orders`, `returns`, `web_sessions`, `inventory_snapshot`) -> **Upload**.
This recreates every `<source>/2026/08/03/<source>.csv` key in a single action.

For a second, smaller **delta** batch exercising the upsert path (a few "correction" rows
plus new orders for a new day):
```
python3 jobs/generate_mock_data.py --out-dir data/mock/delta --mode delta --as-of-date 2026-08-04 --scale 1.0 --seed 42
python3 scripts/stage_for_s3_upload.py --generated-dir data/mock/delta --as-of-date 2026-08-04
```
Upload `dist/raw_upload/`'s (now updated) 7 folders the same way.

### 5.5 Run the workflow

Every run needs `--load_date` set to the date partition you uploaded (§5.4), and optionally a
fixed `--batch_id`. Two places to set this -- see `DEPLOYMENT.md` §7 for full detail on both,
including a worked example. The verified, recommended path:

1. **Glue Console > Workflows (orchestration) > `retail-mdp-dev-workflow`** -> **Details**
   tab -> in the graph, click the **`retail-mdp-dev-start-bronze`** node -> **Edit**.
2. Define `--load_date` (e.g. `2026-08-03`) and, optionally, `--batch_id` (e.g.
   `manual-run-2026-08-03`) -> Save.
3. Back on the workflow, click **Run**.
4. Watch progress under the **History** tab -- each run shows bronze, then silver, then gold
   as they complete. A full run at `--scale 1.0` typically takes a few minutes per job on
   the default `G.1X x 2` workers -- see [§3.1](#31-dpu-budget-for-capacity-restricted-accounts)
   for the DPU math if your account has a capacity cap.
5. For the delta batch: repeat steps 1-3 with `--load_date=2026-08-04`.

(Alternative: the Workflow's own **Edit workflow > Properties > Run properties** accepts the
same two keys, with or without the `--` prefix -- either mechanism works, but if you set both,
the Run properties value takes priority over the trigger's own argument.)

### 5.6 Manual/ad-hoc job test runs (outside the workflow)

Each job can also be run standalone via **Glue Console > Jobs > select job > Run**, adding
**Job parameters** as needed per `config/job_params/*.json`.

> **Never add a Job Parameter with a blank/empty value.** AWS Glue does not correctly pass
> an empty-string parameter value through to the job's argument parser -- the job fails
> immediately with `GlueArgumentError: argument --X: expected one argument`. To use a
> parameter's default, **omit that parameter row entirely** rather than adding it with no
> value. This applies both to ad-hoc parameters you add in the "Run job" dialog and to a
> job's persistent Job Parameters under Job details.

- **Bronze**: run with just `--load_date` (required). Optionally add `--batch_id` with a
  real fixed value (e.g. `manual-test-001`) so you can reuse that exact value for Silver's
  standalone run next, instead of digging the auto-generated UUID out of CloudWatch Logs.
- **Silver/Gold**: when run via the Workflow, `--batch_id`/`--load_date` are picked up
  automatically from the workflow's run properties -- don't add them as parameters at all.
  Run standalone (outside the Workflow), they're required: add `--batch_id` (matching
  Bronze's exactly) and `--load_date` as real values.

---

## 6. Validation checklist (Athena)

**One-time setup:** Athena needs an S3 location to write query results to. `01-storage.yaml`
creates a bucket for exactly this (`retail-mdp-dev-athena-query-results-<account-id>`,
30-day lifecycle expiration since results are just re-run to regenerate) -- point your
workgroup at it once: **Athena Console > Administration > Workgroups > `primary`
(or your own) > Edit > Query result configuration > Location of query result** ->
`s3://retail-mdp-dev-athena-query-results-<account-id>/`.

Then open **Athena Console** (or **Glue Console > Data Catalog > query editor**), select that
workgroup, and run:

```sql
-- 1. Bronze landed the batch
SELECT batch_id, load_id, count(*) AS rows, min(ingested_at), max(ingested_at)
FROM retail_bronze.orders
GROUP BY batch_id, load_id
ORDER BY max(ingested_at) DESC;

-- 2. Silver row counts and quarantine counts line up with expectations
SELECT 'fact_order_lines' AS tbl, count(*) FROM retail_silver.fact_order_lines
UNION ALL
SELECT 'fact_order_lines_rejects', count(*) FROM retail_silver.fact_order_lines_rejects
UNION ALL
SELECT 'fact_returns', count(*) FROM retail_silver.fact_returns
UNION ALL
SELECT 'fact_returns_rejects', count(*) FROM retail_silver.fact_returns_rejects;

-- 3. Quarantine reason codes present (should include the dirty types the
--    mock generator injects: NULL_PRODUCT_ID, OUT_OF_RANGE_QTY, etc.)
SELECT reject_reasons, count(*) FROM retail_silver.fact_order_lines_rejects
GROUP BY reject_reasons ORDER BY 2 DESC;

-- 4. No duplicate business keys in Silver (should return 0 rows)
SELECT order_id, order_line_id, count(*) c
FROM retail_silver.fact_order_lines
GROUP BY order_id, order_line_id HAVING count(*) > 1;

-- 5. Referential integrity: every fact_order_lines.product_id resolves (0 rows)
SELECT f.product_id FROM retail_silver.fact_order_lines f
LEFT JOIN retail_silver.dim_product p ON f.product_id = p.product_id
WHERE p.product_id IS NULL;

-- 6. PII is actually masked (customer_email_hash present, no raw email column)
SELECT customer_id, customer_email_hash FROM retail_silver.fact_order_lines LIMIT 5;

-- 7. Gold KPIs look sane for the loaded date
SELECT * FROM retail_gold.kpi_sales_daily WHERE kpi_date = DATE '2026-08-03' ORDER BY net_sales DESC LIMIT 20;
SELECT * FROM retail_gold.kpi_returns_daily WHERE kpi_date = DATE '2026-08-03';
SELECT * FROM retail_gold.kpi_promo_lift WHERE kpi_date = DATE '2026-08-03';

-- 8. Observability: recent run history, durations, rejected counts
SELECT job_name, table_name, status, rows_read, rows_written, rows_rejected,
       duration_seconds, run_ts
FROM retail_gold.pipeline_run_log
ORDER BY run_ts DESC LIMIT 50;

-- 9. Schema evolution sanity check after adding a column (see 6.1)
DESCRIBE retail_gold.kpi_sales_daily;
```

For a single consolidated pass/fail report across completeness, uniqueness, and referential
integrity (instead of running the queries above one at a time), you have two no-extra-cost
options -- **neither requires Glue Interactive Sessions**, which are disabled in some
capacity-restricted accounts:

- Re-run `gold_job` with `--run_dq_checks true` -- it runs the same checks inline at the end
  of its own Spark session (see `jobs/common/dq_report.py`) and logs the report to
  CloudWatch (that job's log group), at no extra DPU-count/job-count cost.
- If you have spare job-count/DPU budget, upload `tests/data_quality_checks.py` next to the
  three ETL scripts and run it as its own lightweight Glue job (same
  `--silver_database`/`--gold_database` parameters) -- see the file's docstring.

### 6.1 Demonstrating schema evolution

In an Athena query or Glue interactive session:
```sql
ALTER TABLE retail_gold.kpi_sales_daily ADD COLUMNS (channel_grouping STRING);
```
Rerun `gold_job` for any date -- old rows read back with `channel_grouping = NULL`, new rows
populate whatever the job is changed to write (add a `.withColumn("channel_grouping", ...)`
in `compute_kpi_sales_daily` if you want to actually populate it). This is a safe additive
change; `common/iceberg_utils.add_column_if_not_exists` exists for doing this
programmatically from a job. Renames/drops are deliberately **not** automated by that helper
-- do those as reviewed, explicit DDL, since they can break existing readers/dashboards.

---

## 7. Troubleshooting guide

| Symptom | Likely cause | Fix |
|---|---|---|
| `GlueArgumentError: argument --X: expected one argument` | A Job Parameter was added with a **blank/empty value** -- AWS Glue doesn't pass empty-string parameter values through to the job's argument parser correctly | Remove that parameter row entirely instead of leaving it blank (see 5.6). Check both the job's persistent Job Parameters (Job details) and any ad-hoc overrides in the "Run job" dialog |
| Job fails at import: `ModuleNotFoundError: No module named 'common'` | `--extra-py-files` zip wasn't built with `common/` as the zip root, or wasn't uploaded to the exact path the job expects | Re-zip so `common/__init__.py` is at the zip's top level; confirm it's at `s3://<scripts-bucket>/common/common.zip` |
| `AnalysisException: Table or view not found` in Silver/Gold | Bronze (or Silver) hasn't run yet for this batch, or ran against a different `bronze_database`/`silver_database` parameter than what this job is using | Confirm all 3 jobs' `--*_database` parameters match the `03-catalog.yaml` database names (`retail_bronze`/`retail_silver`/`retail_gold`); run bronze before silver before gold |
| Silver/Gold job fails: `Could not resolve batch_id/load_date` | Job was run standalone (not via the Workflow) without `--batch_id`/`--load_date` set | Either run it from the Workflow, or set both job parameters explicitly for a standalone test (see 5.6) |
| `AccessDeniedException` on `glue:PutWorkflowRunProperties` | IAM role missing the workflow run-properties permissions | Confirm `02-iam.yaml` deployed successfully and includes the `WorkflowRunPropertiesHandoff` statement; redeploy stack 2 if it predates that addition |
| CSV read fails immediately (FAILFAST) | A source file has a genuinely malformed row (wrong column count, unescaped quote) | Either fix the source file, or switch `--csv_parse_mode` to `PERMISSIVE`/`DROPMALFORMED` if some malformed rows are expected/acceptable for this load |
| MERGE INTO fails: `[FEATURE_NOT_SUPPORTED] MERGE INTO ... requires format version 2` | An existing Iceberg table was created before `format-version=2` was the default (shouldn't happen with these scripts, but possible if a table was manually created) | `ALTER TABLE ... SET TBLPROPERTIES ('format-version'='2')` |
| `MERGE INTO` fails: multiple source rows match one target row | Source view passed to `merge_into()` wasn't deduplicated by the merge key first | Every call site in `silver_job.py` already dedupes before merging -- if you add a new table, call `dedup_keep_deterministic` before `merge_into` |
| Athena query returns stale/no data after a job succeeded | Iceberg snapshot committed, but you're querying a cached/older Athena engine version, or looking at the wrong workgroup's data catalog | Confirm the Athena workgroup uses the Glue Data Catalog (default) and Athena engine v3; re-run the query |
| Freshness SLA alarm firing immediately after deploy | Expected -- `TreatMissingData: breaching` means "no successful run yet" also counts as breaching | Run the workflow once; the alarm clears (OKActions fire) once `kpi_promo_lift` succeeds |
| S3 bucket policy blocks console file preview/download over plain HTTP | `DenyInsecureTransport` bucket policies (by design) | Access buckets only via the AWS Console/HTTPS, which already satisfies `aws:SecureTransport=true` |
| `AccessDenied` reading the PII salt secret | `--pii_salt_secret_name` doesn't match the secret `02-iam.yaml` created, or the role's `PiiSaltSecretAccess` policy predates the secret | Confirm the parameter equals `retail-mdp-<env>-pii-salt` (or the IAM stack's `PiiSaltSecretName` Output) |

---

## 8. Rollback / cleanup

Delete stacks in **reverse** order (6 -> 1) from **CloudFormation Console > Stacks > select
stack > Delete**:

1. `retail-mdp-dev-monitoring`
2. `retail-mdp-dev-workflow`
3. `retail-mdp-dev-jobs`
4. `retail-mdp-dev-catalog` -- **this deletes the Glue databases but not the Iceberg
   table data/metadata sitting in S3** (Glue database deletion doesn't touch S3 objects).
5. `retail-mdp-dev-iam`
6. `retail-mdp-dev-storage` -- **will fail to delete non-empty buckets.** Before deleting
   this stack: S3 Console > each `retail-mdp-dev-*` bucket > **Empty** (this also removes
   all object versions, since several buckets have versioning enabled) -> confirm -> then
   retry stack deletion. The `raw`/`warehouse`/`scripts`/`logs` buckets use
   `DeletionPolicy: Retain`, so CloudFormation will leave them in place even after a
   successful stack delete if they still contain objects -- delete them manually from S3
   Console afterward if you want a full teardown. `temp` uses `DeletionPolicy: Delete` and
   will be removed automatically once empty.

Buckets protected by `DeletionPolicy: Retain` were made that way deliberately (raw
landing data, Iceberg table data, deployed scripts, and access logs are exactly the kind of
thing an accidental stack deletion shouldn't silently destroy) -- treat their manual deletion
as a separate, deliberate step, not something to script away.

---

## 9. Security & governance

- **Encryption**: SSE-S3 (AES-256) on every bucket; TLS-only bucket policies deny any
  non-HTTPS request. PII salt lives in Secrets Manager, never in code or job parameters.
- **Least privilege**: a single Glue execution role, scoped to exactly this project's 4
  data buckets, 3 Glue databases (catalog + database + table-level ARNs, not `*`), its own
  log groups, its own workflow, and the PII salt secret -- no `AWSGlueServiceRole` managed
  policy, no wildcard resource ARNs.
- **PII masking**: `customer_email` is hashed (salted SHA-256) in Silver and dropped from
  every table downstream of Bronze. For a stricter posture, put Bronze behind a separate,
  more restricted IAM boundary than Silver/Gold (this build uses one shared role across all
  three layers for simplicity -- splitting into per-layer roles is a straightforward
  extension of `02-iam.yaml`'s existing policy structure).
- **No hardcoded secrets**: verified by construction -- the only secret (PII salt) is
  fetched at runtime via `common/secrets_utils.get_secret_string`, referenced only by name
  in job parameters.
- **Auditability**: Bronze is append-only (full raw history, replayable); every job run
  emits a structured JSON log line and a row in `retail_gold.pipeline_run_log` (job, table,
  batch_id, load_id, status, row counts, duration, watermark).

---

## 10. Local development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt

python jobs/generate_mock_data.py --out-dir data/mock/full --mode full --as-of-date 2026-08-03 --scale 0.3 --seed 42
python -m pytest tests/test_mock_data.py -v
python -m pyflakes jobs/*.py jobs/common/*.py tests/*.py
for f in infrastructure/cloudformation/*.yaml; do cfn-lint "$f"; done
python scripts/build_common_zip.py   # writes dist/common.zip -- see DEPLOYMENT.md
```

`requirements-dev.txt` pins only local validation tooling (`pytest`, `cfn-lint`,
`pyflakes`, `pyyaml`). The Glue jobs themselves (`bronze_job.py`/`silver_job.py`/
`gold_job.py`) depend on the `awsglue` module, which only exists in the AWS Glue runtime (or
the Glue ETL local development Docker image) -- they are validated in this repo via
`py_compile`/`pyflakes` and are meant to be exercised for real via the Console runbook in
section 5 (or the checklist in [`DEPLOYMENT.md`](DEPLOYMENT.md)), not run directly on a
laptop.
