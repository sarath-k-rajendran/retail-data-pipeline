# Retail Medallion Data Pipeline — Build Plan

Principal-engineer build plan for a production-grade retail Medallion pipeline on AWS:
Glue ETL (Spark/Python) + Iceberg + S3 + Glue Data Catalog + CloudFormation + Glue Workflows.
No Terraform. Console-driven runbook (no AWS CLI required).

This plan is executed **incrementally, phase by phase**. Each phase has a scope, deliverables,
and an exit checklist. We do not start phase N+1 until phase N's exit checklist is met (or the
user explicitly says to jump ahead).

---

## Phase 0 — Foundations & Design Decisions

Lock down decisions that every later phase depends on, so we don't rework infra or schemas later.

**Decisions to finalize:**
- Repo/folder layout (final tree below).
- Naming convention: `retail-mdp-{env}-{resource}` (e.g. `retail-mdp-dev-raw`).
- AWS region assumption (single region, parameterized in CFN, default `us-east-1`).
- Environments: single `dev` env for this build (params support `env` for future prod).
- S3 layout:
  - `s3://retail-mdp-{env}-raw/{source}/{yyyy}/{mm}/{dd}/*.csv` — landing zone for mock data
  - `s3://retail-mdp-{env}-warehouse/bronze/{table}/` — Iceberg bronze tables
  - `s3://retail-mdp-{env}-warehouse/silver/{table}/` — Iceberg silver tables
  - `s3://retail-mdp-{env}-warehouse/gold/{table}/` — Iceberg gold tables
  - `s3://retail-mdp-{env}-warehouse/quarantine/{table}/` — Silver rejects
  - `s3://retail-mdp-{env}-scripts/jobs/` — Glue script assets
  - `s3://retail-mdp-{env}-temp/` — Glue temp dir, Spark checkpoints, job bookmarks scratch
  - `s3://retail-mdp-{env}-logs/` — access logs (S3 server access logging target)
- Glue Catalog databases: `retail_bronze`, `retail_silver`, `retail_gold`.
- Business keys per entity (drives Silver MERGE + dedup):
  - orders line: `order_id + order_line_id`
  - returns: `return_id`
  - product: `product_id`
  - store: `store_id`
  - promotion: `promo_id`
  - web session: `session_id`
  - inventory snapshot: `store_id + product_id + snapshot_date` (a snapshot fact, not a
    current-state dim — Silver upserts one row per key per day rather than a single
    latest-wins row, so history of on-hand levels is preserved for turnover calculations)
- Incremental logic: no CDC columns; simple upsert (`business_key exists -> UPDATE else INSERT`)
  per the simplified spec. Bronze append-only; Silver MERGE INTO current-state.
- Deterministic dedup rule for MERGE prep: within a batch, keep the row with the greatest
  `(source_file, ingested_at)` per business key (documented, deterministic, no reliance on
  event-time columns since none are guaranteed).
- **Partitioning strategy (finalized):** partition granularity is chosen to match each
  table's dominant filter predicate, while avoiding over-partitioning small tables (which
  creates excessive tiny Iceberg metadata/data files and hurts write/compaction efficiency).
  Iceberg hidden partitioning is used throughout (`days(col)` transforms) so consumers never
  need to reference the partition column directly in queries.

  | Layer  | Table | Partition | Rationale |
  |---|---|---|---|
  | Bronze | orders | `days(order_ts)` | Business event date; matches Silver/Gold access pattern and keeps late-arriving batches for old dates from fragmenting into a single "today" partition |
  | Bronze | returns | `days(return_ts)` | Same as orders |
  | Bronze | web_sessions | `days(session_ts)` | Same as orders |
  | Bronze | inventory_snapshot | `days(snapshot_date)` | Snapshot cadence is daily; queries filter by snapshot date range |
  | Bronze | product, store, promotion | unpartitioned | Small reference tables (hundreds of rows); partitioning would create more metadata files than data files |
  | Silver | fact_order_lines | `days(order_date)` | Primary filter for KPI/date-range queries; aligns with Gold rollups |
  | Silver | fact_returns | `days(return_date)` | Same reasoning |
  | Silver | inventory_snapshot | `days(snapshot_date)` | Preserves daily grain needed for stockout rate / turnover |
  | Silver | dim_product, dim_store, dim_calendar | unpartitioned | Current-state dims, small row counts (dim_calendar ~thousands of rows at most) |
  | Silver | web_sessions | `days(session_date)` | Matches conversion KPI's daily grain |
  | Gold | kpi_sales_daily, kpi_returns_daily, kpi_inventory_daily, kpi_promo_lift | `days(kpi_date)` | All Gold marts are queried by date range first; region/category/channel are left as regular (non-partition) columns and pruned via Iceberg column stats/min-max indexes rather than physical partitioning, since partitioning by all four dimensions would explode small aggregate tables into excessive tiny files |

  Bronze partitions on **business event date** (not ingestion date) deliberately: it keeps
  backfills and delta reprocessing of a past date range scoped to a small, predictable set of
  partitions instead of scattering across "today's" ingestion partition, and it matches how
  Silver/Gold will filter — so a full day's lineage (bronze→silver→gold) lives in the same
  partition value across layers.
- Idempotency control: `batch_id` (UUID per run) + `load_id` (deterministic, e.g.
  `{table}_{load_mode}_{yyyymmddHHMMSS}`) recorded on every Bronze row; Silver MERGE is safe
  to rerun because it's keyed on business_key, not on batch_id.
- IAM: one least-privilege Glue execution role, scoped to the pipeline's own buckets/prefixes
  and Glue Catalog databases only.
- Encryption: SSE-S3 (or SSE-KMS if user wants a CMK — default to SSE-S3 for simplicity,
  document how to swap to KMS) on all buckets; TLS-only bucket policies.
- PII fields identified (customer_email, customer_name if present in orders/web sessions) and
  masking pattern chosen (hash or tokenize in Silver, raw preserved only in Bronze with
  restricted access).

**Deliverable:** This `plan.md` (you're reading it) + confirmed decisions above. No code yet.

**Confirmed by user (2026-08-03):**
- Region: `us-east-1` (default CFN parameter, overridable)
- Encryption: SSE-S3 (AWS-managed keys) on all buckets
- Scope: web_sessions IS included from the start — mock generator, bronze/silver tables,
  and a conversion-rate cut in Gold KPIs are all in scope for the initial build, not deferred

**Exit checklist:**
- [x] User confirms region/env/naming defaults (or overrides them)
- [x] Business keys and partitioning strategy finalized (table above) — engineering call made
      per standard practice (match partition to dominant filter predicate, avoid
      over-partitioning small tables); flag here if you want a different scheme
- [x] User confirms PII masking approach (SSE-S3 encryption + hash-based masking in Silver, as designed)

**Phase 0 status: COMPLETE.**

---

## Phase 1 — Repo Scaffolding & Mock Data Generator

**Scope:** Create the full repo skeleton and the local mock data generator — the only phase
with zero AWS dependency, so it's fully testable locally first.

**Deliverables:**
```
retail-data-pipeline/
├── plan.md
├── README.md                                  (stub, filled in Phase 7)
├── infrastructure/
│   └── cloudformation/
│       ├── 01-storage.yaml
│       ├── 02-iam.yaml
│       ├── 03-catalog.yaml
│       ├── 04-glue-jobs.yaml
│       ├── 05-workflow.yaml
│       └── 06-monitoring.yaml
├── jobs/
│   ├── generate_mock_data.py
│   ├── bronze_job.py
│   ├── silver_job.py
│   ├── gold_job.py
│   └── common/
│       ├── __init__.py
│       ├── iceberg_utils.py
│       ├── dq_utils.py
│       └── logging_utils.py
├── config/
│   └── job_params/
│       ├── bronze_params.json
│       ├── silver_params.json
│       └── gold_params.json
├── tests/
│   ├── data_quality_checks.py
│   └── test_mock_data.py
└── data/
    └── mock/                                  (generator output, gitignored)
```

- `jobs/generate_mock_data.py`: pure-Python (pandas/faker or stdlib random), generates:
  - `product.csv` (~500 SKUs: product_id, category, sub_category, brand, cost, list_price, active_flag)
  - `store.csv` (~30 stores: store_id, region, state, city, channel=STORE/ONLINE, open_date)
  - `promotion.csv` (~50 promos: promo_id, product_id, discount_pct, start_date, end_date)
  - `orders.csv` (order header+lines flattened: order_id, order_line_id, order_ts, store_id,
    product_id, promo_id nullable, qty, unit_price, discount_amt, channel, customer_id,
    customer_email)
  - `returns.csv` (return_id, order_id, order_line_id, return_ts, qty, reason_code)
  - `web_sessions.csv` (optional: session_id, customer_id, session_ts, store_id/channel,
    converted_flag, order_id nullable)
  - Also emits a **delta batch** variant (subset of updated + new rows) to exercise the
    upsert path, and an **inventory snapshot** feed if needed for stockout/turnover KPIs
    (store_id, product_id, snapshot_date, on_hand_qty, on_order_qty).
  - Deliberately injects a small % of dirty rows (nulls in required fields, bad dates,
    duplicate business keys, malformed CSV lines) to exercise DQ/quarantine logic later.
  - CLI args: `--out-dir`, `--seed`, `--scale`, `--as-of-date`, `--mode full|delta`.
- `tests/test_mock_data.py`: sanity checks (row counts, key uniqueness within a full batch,
  referential integrity of FKs) run locally with pytest.

**Exit checklist:**
- [ ] `python jobs/generate_mock_data.py --mode full` produces all CSVs under `data/mock/`
- [ ] `python jobs/generate_mock_data.py --mode delta` produces a smaller delta batch
- [ ] pytest sanity tests pass locally
- [ ] Spot-checked CSVs manually for realistic distributions (dates, prices, FKs)

---

## Phase 2 — CloudFormation: Storage, IAM, Catalog

**Scope:** IaC for everything that must exist before any Glue job can run.

**Deliverables:**
- `01-storage.yaml`: raw/warehouse/scripts/temp/logs buckets, versioning, SSE, TLS-only
  bucket policy, lifecycle rules (e.g. expire temp/ after N days, transition logs to IA),
  public access block, S3 access logging to the logs bucket.
- `02-iam.yaml`: Glue execution role + managed/inline least-privilege policies scoped to the
  above buckets and to `glue:*` on the three catalog databases only (plus
  `lakeformation`/Glue Iceberg permissions as needed), CloudWatch Logs write permissions.
- `03-catalog.yaml`: Glue Catalog databases `retail_bronze`, `retail_silver`, `retail_gold`
  (Iceberg tables themselves are created by the Spark jobs at first run via
  `CREATE TABLE ... USING iceberg`, not by CFN — document this explicitly).
- All templates parameterized (`Env`, `ProjectName`, `AdminPrincipalArn` if needed) with
  `Outputs` (bucket names/ARNs, role ARN, DB names) for cross-stack `Fn::ImportValue`.

**Exit checklist:**
- [ ] Templates pass `aws cloudformation validate-template` equivalent (cfn-lint if available,
      else manual review) — validation done by user in Console since no CLI in runbook
- [ ] Stack dependency order documented (storage → iam → catalog)
- [ ] Outputs/exports named consistently for later stacks to import

---

## Phase 3 — Bronze Layer

**Scope:** `jobs/bronze_job.py` + its CFN job resource + params.

**Deliverables:**
- Glue job (Python Shell disabled; Glue ETL Spark, Glue version 4.0+ for native Iceberg
  support) reading raw CSVs per source from S3, writing append-only Iceberg tables:
  `retail_bronze.orders`, `.returns`, `.product`, `.store`, `.promotion`,
  `.web_sessions` (optional), `.inventory_snapshot`.
- Adds `ingested_at`, `source_file`, `batch_id`, `load_id` columns.
- `csv_parse_mode` job parameter → PERMISSIVE / DROPMALFORMED / FAILFAST wired into the
  Spark CSV reader options; malformed-row counts logged.
- No filtering/business logic — raw fidelity preserved.
- Idempotent append: reruns of the same `batch_id` do not duplicate (guard: check if
  `batch_id` already present in target table before writing, skip/warn if so — documented
  replay override param `force_reload=true`).
- `config/job_params/bronze_params.json` documents every `--param` the job accepts.

**Exit checklist:**
- [ ] Bronze job runs locally against sample CSVs via `pytest`-driven local SparkSession
      (glue local dev) OR documented as Console-only validated in Phase 6
- [ ] Bronze Iceberg tables created with correct schema + partitioning documented

---

## Phase 4 — Silver Layer

**Scope:** `jobs/silver_job.py` — the most complex job.

**Deliverables:**
- Reads latest Bronze batch (by `batch_id`/`load_id`), per source.
- Type/timezone standardization (UTC canonical timestamps), trimming, casing rules.
- Validation ruleset → quarantine table per source with `reject_reason` codes (null required
  field, bad FK, invalid enum, negative qty/price, duplicate business key survivor logic).
- Deterministic in-batch dedup by business key (rule from Phase 0) before MERGE.
- `MERGE INTO` Iceberg silver tables on business_key (Spark SQL `MERGE INTO`), giving
  simple upsert semantics per the simplified incremental spec.
- Reference joins: orders/returns enriched with product/store/promo dims to produce:
  - `silver.fact_order_lines`, `silver.fact_returns`
  - `silver.dim_product`, `silver.dim_store`, `silver.dim_calendar` (calendar generated
    programmatically, not sourced from raw).
- PII masking applied here (e.g. `customer_email` hashed with salted SHA-256) — raw value
  stays only in Bronze.
- Row count + rejected count + duration metrics emitted (Phase 6 wires these to CloudWatch).

**Exit checklist:**
- [ ] Full-load then delta-load run twice confirms idempotent upsert (no dupes, updates land)
- [ ] Quarantine table captures injected dirty rows from mock generator with correct reasons
- [ ] dim_calendar spans full mock data date range

---

## Phase 5 — Gold Layer

**Scope:** `jobs/gold_job.py` — KPI marts.

**Deliverables:**
- Aggregated Iceberg tables by day/region/category/channel:
  - `gold.kpi_sales_daily` (Net Sales, Gross Sales, Discount %, Gross Margin %, AOV,
    Units per Transaction)
  - `gold.kpi_returns_daily` (Return Rate %)
  - `gold.kpi_inventory_daily` (Stockout Rate %, Inventory Turnover — from
    `inventory_snapshot`)
  - `gold.kpi_promo_lift` (Promo Lift % — promo vs. non-promo baseline comparison)
- Each KPI formula documented inline (comment) with numerator/denominator definition.
- Full recompute vs. incremental-by-partition strategy decided and documented (likely:
  gold recomputes affected date partitions via `INSERT OVERWRITE`/Iceberg partition
  replace for the batch's date range — atomic per Iceberg semantics).

**Exit checklist:**
- [ ] KPI values manually spot-checked against mock data by hand-calculation for one day/store
- [ ] Schema evolution demo: add a compatible column (e.g. `channel_grouping`) to one gold
      table via `ALTER TABLE ... ADD COLUMN`, confirm old data reads back with nulls, confirm
      Athena compatibility

---

## Phase 6 — Orchestration & CloudFormation Wiring

**Scope:** `04-glue-jobs.yaml`, `05-workflow.yaml`, `06-monitoring.yaml` + end-to-end wiring.

**Deliverables:**
- `04-glue-jobs.yaml`: `AWS::Glue::Job` resources for bronze/silver/gold (script location =
  scripts bucket, default arguments incl. `--datalake-formats=iceberg`, warehouse path,
  temp dir, Glue version, worker type/count, job bookmarks off by default since we control
  idempotency via batch_id).
- `05-workflow.yaml`: `AWS::Glue::Workflow` + `AWS::Glue::Trigger` chain:
  `ON_DEMAND/SCHEDULED trigger → bronze job → (success) → silver job → (success) → gold job`,
  with a conditional trigger type and failure notification path.
- `06-monitoring.yaml`: CloudWatch Log Groups (or reuse Glue-managed), CloudWatch Alarms on
  job failure (via EventBridge rule on Glue job state change → SNS topic) and a freshness
  SLA alarm (e.g. custom metric/last-success-timestamp check — document simplest viable
  approach, e.g. EventBridge scheduled check via a small Lambda or a Glue job assertion vs.
  full Lambda+metric pipeline decision made explicitly here, keeping it CFN-only).
- Upload mechanics: scripts must be manually uploaded to the scripts bucket before job
  creation (documented in runbook) — CFN doesn't package/upload local files, only defines
  resources.

**Exit checklist:**
- [ ] Workflow triggers bronze→silver→gold in Console with correct dependency conditions
- [ ] Simulated job failure (bad param) triggers the failure alarm/notification
- [ ] Rerunning workflow with same data is a no-op / safe (idempotency proven end-to-end)

---

## Phase 7 — Data Quality, Observability & Tests

**Scope:** `tests/data_quality_checks.py`, `jobs/common/dq_utils.py`, `jobs/common/logging_utils.py`.

**Deliverables:**
- Reusable DQ check library: completeness (non-null required cols), validity (enum/range/
  regex), uniqueness (business key), referential integrity (FK exists in dimension) —
  usable both inside Glue jobs (Silver quarantine logic) and standalone via Athena/PyIceberg
  for post-hoc validation.
- Observability helper: structured JSON log lines per job run (row counts in/out/rejected,
  duration, watermark/batch_id) — written to CloudWatch via standard Glue logging, plus
  optionally persisted to a `gold.pipeline_run_log` Iceberg table for a self-service audit
  trail.
- `tests/data_quality_checks.py`: standalone script runnable post-deployment (via Athena
  JDBC/boto3 or Glue interactive session) to assert row counts, null rates, dupe rates,
  and referential integrity across bronze/silver/gold after a real run.

**Exit checklist:**
- [ ] DQ checks catch every dirty-row type the mock generator injects
- [ ] `pipeline_run_log` (or equivalent) populated after a real workflow run

---

## Phase 8 — Documentation & Runbook

**Scope:** Final `README.md` — the only phase primarily about writing, not code.

**Deliverables:**
- Architecture diagram (ASCII or Mermaid) + assumptions section.
- CloudFormation design summary (stack order, params, outputs, what's manual vs. IaC).
- Console-only runbook: create stacks in order → upload scripts to S3 → upload mock data to
  S3 (Console upload steps, no CLI) → create Glue jobs (if not fully CFN'd, note manual
  script association) → run workflow → validate in Athena.
- Validation checklist (Athena queries for each layer/table).
- Troubleshooting guide (common Glue/Iceberg errors: schema mismatch, concurrent write
  conflicts, S3 permission errors, catalog table already exists, MERGE INTO requires
  Iceberg format-version 2, etc.).
- Rollback/cleanup: stack deletion order (reverse of creation), S3 bucket emptying
  requirement before delete, Iceberg table drop vs. data retention note.

**Exit checklist:**
- [ ] A fresh reader could deploy from zero using only the README + Console
- [ ] Rollback steps verified against actual resource dependency graph

---

## Build Order Summary

```
Phase 0  Design decisions (this doc)             ← confirm now
Phase 1  Repo scaffold + mock data generator      ← no AWS needed, build/test first
Phase 2  CFN: storage, IAM, catalog databases
Phase 3  Bronze Glue job
Phase 4  Silver Glue job
Phase 5  Gold Glue job
Phase 6  CFN: Glue jobs, workflow/triggers, monitoring
Phase 7  DQ library + observability + tests
Phase 8  README / runbook / troubleshooting
```

Each phase is a self-contained PR-sized chunk of work. We'll pause after each phase for
review before moving to the next.
