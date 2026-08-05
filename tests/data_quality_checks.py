"""Standalone, post-hoc data-quality validation for the retail Medallion
pipeline. Asserts completeness, uniqueness, validity, and referential
integrity across the already-persisted Silver/Gold Iceberg tables --
independent of whatever ran (or didn't run) inside silver_job.py's inline
quarantine step.

This is a thin wrapper around jobs/common/dq_report.py, which holds the
actual check logic and is shipped to every Glue job via common.zip. It's
split out this way deliberately: **capacity-restricted AWS accounts often
disable Glue Interactive Sessions and cap you at a small number of job
definitions**, which rules out both of the "obvious" ways to run a
standalone validation script. Given that constraint, prefer one of these
instead, in order:

  1. Athena SQL (no Glue job, no session, no DPU cost at all) -- see
     README.md Sec. 6 for the exact queries covering the same ground.
  2. Pass --run_dq_checks true to gold_job.py -- it calls
     common.dq_report.run_all() inline at the end of its own run, using
     the Spark session it already has, so it costs no extra DPU allocation
     or job-count budget.
  3. Only if your environment has spare job-count/DPU budget: submit this
     file itself as its own lightweight Glue Spark job (upload it next to
     bronze_job.py/silver_job.py/gold_job.py, same --extra-py-files, same
     --silver_database/--gold_database parameter names).

__main__ below wires up option 3 via getResolvedOptions.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs"))

from common.dq_report import run_all  # noqa: E402

if __name__ == "__main__":
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["silver_database", "gold_database"])
    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark_session = glue_ctx.spark_session

    dq_failures = run_all(spark_session, args["silver_database"], args["gold_database"])
    if dq_failures:
        raise SystemExit(f"{len(dq_failures)} data quality check(s) failed -- see report above")
