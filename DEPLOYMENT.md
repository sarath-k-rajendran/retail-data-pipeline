# Deployment Checklist

A self-contained, ordered checklist for deploying this pipeline to a real AWS account.
For the "why" behind any step, see `README.md` (architecture, CloudFormation design,
troubleshooting). This file is the "just tell me what to click/run, in order" version.

Nothing here uses the AWS CLI -- every AWS-facing step is a Console action. The only local
commands are Python (mock data generation + packaging the shared library).

---

## 0. Do you need to build an archive file? Yes.

Glue's **`--extra-py-files`** job argument (used by all three jobs to load
`jobs/common/*.py`) accepts individual `.py` files or a **`.zip` archive** -- it does not
accept a plain S3 "folder" of loose files. So before the jobs can run, you must zip
`jobs/common/` (with `common/` itself as the zip root) into `common.zip` and upload it to S3.
Step 3 below does this with a provided script -- you don't need to build the zip by hand.

---

## 1. Local environment setup

```bash
cd retail-data-pipeline
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
```

Sanity-check everything still passes before you touch AWS:

```bash
python -m pytest tests/test_mock_data.py -v
python -m pyflakes jobs/*.py jobs/common/*.py tests/*.py
for f in infrastructure/cloudformation/*.yaml; do cfn-lint "$f"; done
```

All three should be clean (8 tests passed, no pyflakes output, no cfn-lint output). If
anything fails here, stop and fix it before deploying -- these are cheap, fast, local checks
that catch far more than anything you'll see in the CloudFormation Console.

Remember to `source .venv/bin/activate` again in any new terminal session before running the
Python commands in this checklist.

---

## 2. AWS prerequisites

- [ ] An AWS account/IAM identity with Console access to: CloudFormation, S3, Glue, IAM,
      Secrets Manager, CloudWatch, SNS, EventBridge, Athena.
- [ ] Decide your region (all examples below assume the templates' default, `us-east-1`;
      change the region selector top-right in the Console if you want a different one --
      just be consistent across every step).
- [ ] Decide `ProjectName` (default `retail-mdp`) and `Env` (default `dev`). **Use the exact
      same values for every stack below** -- cross-stack `Fn::ImportValue` lookups are keyed
      on these two parameters and will fail to resolve if they don't match.
- [ ] If your account is capacity-restricted (limited Glue job count / total DPU / DPU-hours,
      Interactive Sessions disabled), read README.md §3.1 first. This build already uses the
      smallest valid worker config for a Spark ETL (`glueetl`) job: `G.1X`, 2 workers/job = 2
      DPU/job (`G.025X` looks smaller but AWS rejects it outright for `glueetl` jobs -- it's
      streaming-only). 3 job definitions x 2 DPU = 6 DPU of *static* configured capacity, but
      since the jobs run strictly sequentially (never concurrently), actual usage never
      exceeds 2 DPU at any instant -- see README.md §3.1 for what to do if your account
      enforces a hard static-sum cap instead. Don't raise `NumberOfWorkers` above 2 in step 4
      until you've checked your actual quota.

---

## 3. Build the common.zip package

```bash
source .venv/bin/activate   # if not already active
python scripts/build_common_zip.py
```

This writes `dist/common.zip` containing `common/__init__.py`, `common/iceberg_utils.py`,
`common/dq_utils.py`, `common/logging_utils.py`, `common/schemas.py`,
`common/secrets_utils.py`, `common/workflow_utils.py`. You'll upload this file in step 5.
Re-run this any time you change a file under `jobs/common/` -- it always overwrites
`dist/common.zip` fresh.

---

## 4. Deploy the CloudFormation stacks (in order)

For each stack: **CloudFormation Console > Stacks > Create stack > With new resources
(standard) > Upload a template file** -> choose the file -> **Next** -> enter the stack name
and parameters (`ProjectName=retail-mdp`, `Env=dev`, leave everything else at its default
unless noted) -> **Next** -> **Next** -> check **"I acknowledge that AWS CloudFormation
might create IAM resources"** if shown -> **Submit**. Wait for **CREATE_COMPLETE** (check the
**Events** tab if it fails) before starting the next stack.

- [ ] **1/6** `infrastructure/cloudformation/01-storage.yaml`
      -> stack name `retail-mdp-dev-storage`
- [ ] **2/6** `infrastructure/cloudformation/02-iam.yaml`
      -> stack name `retail-mdp-dev-iam`
- [ ] **3/6** `infrastructure/cloudformation/03-catalog.yaml`
      -> stack name `retail-mdp-dev-catalog`
- [ ] **4/6** `infrastructure/cloudformation/04-glue-jobs.yaml`
      -> stack name `retail-mdp-dev-jobs`
      (this creates the 3 Glue job *definitions*; they won't successfully **run** until step
      5 uploads their scripts -- that's fine, the stack itself still creates successfully)
- [ ] **5/6** `infrastructure/cloudformation/05-workflow.yaml`
      -> stack name `retail-mdp-dev-workflow`
- [ ] **6/6** `infrastructure/cloudformation/06-monitoring.yaml` (optional but recommended)
      -> stack name `retail-mdp-dev-monitoring`
      -> set `AlarmEmail` to your email if you want failure/freshness alerts, then check
      your inbox afterward and confirm the SNS subscription

---

## 5. Upload job scripts and the common.zip package

1. **S3 Console > `retail-mdp-dev-scripts-<your-account-id>`** (find the exact bucket name
   in `retail-mdp-dev-storage`'s **Outputs** tab -> `ScriptsBucketName`).
2. Click **Create folder**, name it `jobs`, **Create folder**. Click into the new `jobs/`
   folder (so you're browsing inside it), then **Upload > Add files** and select
   `jobs/bronze_job.py`, `jobs/silver_job.py`, `jobs/gold_job.py` from your local repo
   (all 3 at once) -> **Upload**.
3. Go back to the bucket root. Click **Create folder**, name it `common`, **Create folder**.
   Click into the new `common/` folder, then **Upload > Add files** and select
   `dist/common.zip` (built in step 3 above) -> **Upload**.

Final S3 layout should be:
```
s3://retail-mdp-dev-scripts-<account-id>/jobs/bronze_job.py
s3://retail-mdp-dev-scripts-<account-id>/jobs/silver_job.py
s3://retail-mdp-dev-scripts-<account-id>/jobs/gold_job.py
s3://retail-mdp-dev-scripts-<account-id>/common/common.zip
```
This matches exactly what `04-glue-jobs.yaml`'s `ScriptLocation` and `--extra-py-files`
already point at -- nothing else to configure.

---

## 6. Generate and upload mock data

```bash
source .venv/bin/activate   # if not already active
python jobs/generate_mock_data.py --out-dir data/mock/full --mode full --as-of-date <TODAY> --scale 0.3 --seed 42
python scripts/stage_for_s3_upload.py --generated-dir data/mock/full --as-of-date <TODAY>
```
Replace `<TODAY>` with today's date as `YYYY-MM-DD`. `--scale 0.3` keeps the first run small
and fast (a few thousand order lines) so any problems surface quickly; bump it toward `1.0`
once you've proven the pipeline runs end to end.

`bronze_job.py` reads each source at exactly `s3://<raw-bucket>/<source>/<yyyy>/<mm>/<dd>/*.csv`
-- **no `raw/` prefix inside the bucket** (the bucket's own name already says "raw"). The S3
Console's Upload dialog has no field to type an arbitrary destination key for loose files, so
`stage_for_s3_upload.py` (just run above) built the correct nested folder structure locally at
`dist/raw_upload/<source>/<yyyy>/<mm>/<dd>/<source>.csv` for you. Now:

1. **S3 Console > `retail-mdp-dev-raw-<your-account-id>`** -- stay at the **bucket root**,
   don't navigate into any prefix.
2. Click **Upload**, then **Add folder** (or drag-and-drop from Finder/Explorer) and select
   **all 7 folders** inside `dist/raw_upload/` at once: `product`, `store`, `promotion`,
   `orders`, `returns`, `web_sessions`, `inventory_snapshot`.
3. Click **Upload**.

This recreates every `<source>/<yyyy>/<mm>/<dd>/<source>.csv` key in one action -- no manual
"Create folder" clicking needed.

---

## 7. Run the workflow

Every run needs `--load_date` set to the date partition you uploaded in step 6 (and,
optionally, a fixed `--batch_id` so you can find/replay this exact run later). There are two
places to set this -- pick **one** (if you set both, Method B takes priority, since Bronze
checks the Workflow's Run properties before its own direct trigger argument):

### Method A: edit the starting trigger's parameters (recommended, verified in Console)

1. **Glue Console > Workflows (orchestration) > `retail-mdp-dev-workflow`**.
2. Open the **Details** tab.
3. In the workflow **graph**, click the node named **`retail-mdp-dev-start-bronze`**
   (the starting trigger -- it's the first node, feeding into the bronze job).
4. Click **Edit**.
5. Under the job action's parameters, define (or update) these two properties:

   | Key | Value | Required? |
   |---|---|---|
   | `--load_date` | `2026-08-04` | Yes -- must match the `yyyy/mm/dd` prefix you uploaded raw files under in step 6 |
   | `--batch_id` | `manual-run-2026-08-04` | No -- omit to auto-generate a UUID each run; set it to something memorable if you want to reuse the exact value later (e.g. for `--force_reload`, or to run Silver standalone against this exact batch) |

   (Example values above -- substitute your actual date/choice of batch_id.)
6. Save the trigger.
7. Back on the workflow page, click **Run**.

### Method B: Workflow "Run properties"

1. **Glue Console > Workflows (orchestration) > `retail-mdp-dev-workflow`**.
2. Click **Edit workflow**. Under **Properties > Run properties**, add the same two rows
   (Key: `load_date` or `--load_date` -- both forms work -- Value: `2026-08-04`; optionally
   `batch_id` / `--batch_id` = `manual-run-2026-08-04`) -> **Save**.
3. Click **Run**.

This sets values on the *workflow* itself (persists across runs until you change it again),
versus Method A which sets them on the *trigger's action* (same idea, different UI path --
whichever you find easier to navigate to is fine).

### Then, either way:

- **History** tab -> watch bronze -> silver -> gold complete in sequence. At `--scale 0.3`
  on the default `G.1X x 2` workers (2 DPU per job; only one job runs at a time, so peak
  concurrent usage is 2 DPU, not 6 -- see README.md §3.1 if your account enforces a static
  sum across job definitions instead), expect a few minutes total.
- If a job fails: click into its run, open the **Error logs** link (goes straight to
  CloudWatch Logs for that job run), fix the root cause, then re-run the workflow (or just
  that job -- see README.md §5.6 for standalone job re-runs).

---

## 8. Validate

- [ ] **One-time Athena setup:** point a workgroup at the query-results bucket stack 1
      created -- **Athena Console > Administration > Workgroups > `primary` (or your own) >
      Edit > Query result configuration > Location of query result** ->
      `s3://retail-mdp-dev-athena-query-results-<account-id>/`. Without this, Athena queries
      fail with "no output location" errors.

Run the Athena queries in `README.md` §6 (row counts, quarantine reason codes, referential
integrity, PII masking, KPI sanity checks, `pipeline_run_log`). At minimum, confirm:

- [ ] `retail_bronze.orders` has rows for your `batch_id`
- [ ] `retail_silver.fact_order_lines` has rows, and `fact_order_lines_rejects` has some too
      (the mock generator deliberately injects ~2% dirty rows)
- [ ] `retail_gold.kpi_sales_daily` has a row for `<TODAY>`
- [ ] `retail_gold.pipeline_run_log` has one row per table per job for this run

---

## 9. Try the delta batch (exercises the upsert path)

```bash
source .venv/bin/activate
python jobs/generate_mock_data.py --out-dir data/mock/delta --mode delta --as-of-date <TOMORROW> --scale 0.3 --seed 42
python scripts/stage_for_s3_upload.py --generated-dir data/mock/delta --as-of-date <TOMORROW>
```
Upload the (now updated) `dist/raw_upload/` folders the same way as step 6, edit the starting
trigger's `--load_date` to `<TOMORROW>`, and run the workflow again. Then re-check
`fact_order_lines` in Athena -- you should see both new rows (new orders for `<TOMORROW>`)
and updated rows (the delta batch's "correction" rows for a handful of `<TODAY>`'s orders,
landing as UPDATEs via MERGE rather than duplicates).

---

## 10. If you need to tear it down

See `README.md` §8 (Rollback / cleanup) -- delete the 6 stacks in **reverse** order and empty
the S3 buckets first (several have `DeletionPolicy: Retain` by design, so they won't vanish
just because the stack does).
