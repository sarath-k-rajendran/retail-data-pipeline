"""Bronze layer Glue ETL job -- raw-fidelity ingestion.

Reads each source CSV feed from
    s3://<raw_bucket>/<source>/<yyyy>/<mm>/<dd>/*.csv
and appends it, essentially as-is, to an Iceberg table in the `retail_bronze`
Glue database, adding only ingestion metadata columns
(ingested_at, source_file, batch_id, load_id). No business filtering,
cleansing, deduplication, or enrichment happens here -- that is Silver's job.
Bronze is append-only so the full raw history is always replayable/auditable.

Required Spark/Iceberg catalog configuration (spark.sql.catalog.glue_catalog.*,
spark.sql.extensions) is supplied via the Glue job's default arguments in
infrastructure/cloudformation/04-glue-jobs.yaml -- this script intentionally
does not set it itself, to keep infra configuration in one place.

Idempotency: before appending a source, the job checks whether batch_id has
already been committed to that Bronze table. A plain rerun of a batch_id that
already landed is a no-op (safe retry). Pass --force_reload true to first
delete that batch_id's rows and re-append (explicit backfill correction).
"""
from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit

from common.iceberg_utils import (
    append_dataframe,
    batch_already_loaded,
    create_table_if_not_exists,
    delete_batch,
)
from common.logging_utils import RunMetrics, default_load_id, get_logger, new_batch_id, persist_run_log
from common.schemas import BRONZE_METADATA_DDL, BRONZE_SOURCES
from common.workflow_utils import get_run_properties, get_run_property, get_workflow_context, put_run_properties

REQUIRED_ARGS = [
    "JOB_NAME",
    "raw_bucket",
    "warehouse_bucket",
    "bronze_database",
    "gold_database",
]
OPTIONAL_ARGS = {
    "load_date": "",                  # YYYY-MM-DD partition of the raw/ landing zone to ingest;
                                       # falls back to the Workflow's "Run properties" (key
                                       # load_date or --load_date) if not passed directly -- see
                                       # resolve_load_date() below. Required one way or the other.
    "csv_parse_mode": "PERMISSIVE",   # PERMISSIVE | DROPMALFORMED | FAILFAST
    "load_mode": "SNAPSHOT",          # DELTA | SNAPSHOT -- tagged into load_id, no behavior branch in Bronze
    "batch_id": "",                   # blank => also checks Workflow Run properties, else generates a fresh UUID
    "force_reload": "false",          # "true" => delete+replace this batch_id if already loaded
    "sources": "",                    # blank => all sources in BRONZE_SOURCES
    "WORKFLOW_NAME": "",              # auto-injected by Glue when run inside a Workflow trigger
    "WORKFLOW_RUN_ID": "",            # auto-injected by Glue when run inside a Workflow trigger
}


def resolve_args(argv: list[str]) -> dict:
    all_arg_names = REQUIRED_ARGS + list(OPTIONAL_ARGS.keys())
    present = {a for a in all_arg_names if f"--{a}" in argv}
    resolved = getResolvedOptions(argv, list(present | set(REQUIRED_ARGS)))
    for key, default in OPTIONAL_ARGS.items():
        resolved.setdefault(key, default)
    return resolved


def count_data_lines(spark, path: str) -> int:
    """Approximates total data rows across all CSV files at `path` (line
    count minus one header line per file). Used only to estimate rows
    silently dropped by DROPMALFORMED, for observability."""
    lines = spark.read.text(path)
    file_count = lines.select(input_file_name().alias("f")).distinct().count()
    return max(0, lines.count() - file_count)


def ingest_source(spark, logger, source: str, spec: dict, raw_bucket: str,
                   warehouse_bucket: str, bronze_database: str, gold_database: str,
                   load_date: str, csv_parse_mode: str, load_mode: str,
                   batch_id: str, load_id: str, force_reload: bool) -> dict:
    yyyy, mm, dd = load_date.split("-")
    path = f"s3://{raw_bucket}/{source}/{yyyy}/{mm}/{dd}/*.csv"
    table_location = f"s3://{warehouse_bucket}/bronze/{source}/"

    metrics = RunMetrics("bronze_job", "bronze", source, batch_id, load_id)
    metrics.set("watermark", load_date)

    create_table_if_not_exists(
        spark, bronze_database, source, f"{spec['ddl']}, {BRONZE_METADATA_DDL}",
        table_location, partition_clause=spec["partition_clause"],
    )

    if not force_reload and batch_already_loaded(spark, bronze_database, source, batch_id):
        logger.info(f"[{source}] batch_id={batch_id} already loaded -- skipping (idempotent rerun)")
        metrics.set("rows_read", 0)
        metrics.set("rows_written", 0)
        metrics.set("rows_rejected", 0)
        metrics.set("skipped_idempotent", True)
        return metrics.log(logger)

    if force_reload:
        delete_batch(spark, bronze_database, source, batch_id)

    reader = spark.read.option("header", "true").option("mode", csv_parse_mode)
    read_schema_ddl = spec["ddl"]
    if csv_parse_mode == "PERMISSIVE":
        reader = reader.option("columnNameOfCorruptRecord", "_corrupt_record")
        read_schema_ddl = f"{spec['ddl']}, _corrupt_record STRING"

    try:
        df = reader.schema(read_schema_ddl).csv(path)
        df = df.withColumn("source_file", input_file_name())
        df.cache()
        rows_read = df.count()
    except Exception as exc:
        # FAILFAST (or any other read failure) -- surface loudly, no partial commit.
        metrics.set("rows_read", 0)
        metrics.set("rows_written", 0)
        metrics.set("rows_rejected", 0)
        metrics.log(logger, status="FAILED", error=str(exc))
        raise

    rows_malformed = 0
    if csv_parse_mode == "PERMISSIVE":
        rows_malformed = df.filter(col("_corrupt_record").isNotNull()).count()
        df = df.drop("_corrupt_record")
    elif csv_parse_mode == "DROPMALFORMED":
        raw_line_estimate = count_data_lines(spark, path)
        rows_malformed = max(0, raw_line_estimate - rows_read)

    bronze_df = (
        df.withColumn("ingested_at", current_timestamp())
          .withColumn("batch_id", lit(batch_id))
          .withColumn("load_id", lit(load_id))
    )

    append_dataframe(bronze_df, bronze_database, source)
    df.unpersist()

    metrics.set("rows_read", rows_read)
    metrics.set("rows_written", rows_read)
    metrics.set("rows_rejected", rows_malformed)
    record = metrics.log(logger)
    persist_run_log(spark, gold_database, f"s3://{warehouse_bucket}/gold", record)
    return record


def main() -> None:
    args = resolve_args(sys.argv)
    logger = get_logger(args["JOB_NAME"])

    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    # Glue auto-injects WORKFLOW_NAME/WORKFLOW_RUN_ID into every job started
    # by a workflow trigger, including the very first one -- so even Bronze
    # can read the workflow's "Run properties" bag (Workflows > select
    # workflow > Edit workflow > Run properties in the Console). This is
    # the easiest-to-find place to set load_date/batch_id before running
    # the workflow. Checked ahead of a direct job/trigger argument, for
    # consistency with how Silver/Gold prioritize this same run-properties
    # bag over their own direct arguments.
    workflow_ctx = get_workflow_context(args)
    run_props = get_run_properties(workflow_ctx["workflow_name"], workflow_ctx["run_id"]) if workflow_ctx else {}

    load_date = get_run_property(run_props, "load_date") or args["load_date"]
    if not load_date:
        raise ValueError(
            "Could not resolve --load_date. Set it either (a) on the starting trigger: "
            "Workflows > select workflow > Details tab > click the start-bronze node in the "
            "graph > Edit > set --load_date (Key: --load_date, Value: YYYY-MM-DD); (b) as a "
            "'Run property' on the Workflow itself: Edit workflow > Properties > Run "
            "properties (Key: load_date or --load_date, Value: YYYY-MM-DD); or (c) as a "
            "direct job parameter for a standalone run outside the workflow."
        )
    batch_id = get_run_property(run_props, "batch_id") or args["batch_id"] or new_batch_id()
    sources = [s.strip() for s in args["sources"].split(",") if s.strip()] or list(BRONZE_SOURCES.keys())
    force_reload = args["force_reload"].strip().lower() == "true"

    logger.info(
        f"Starting bronze_job: load_date={load_date} load_mode={args['load_mode']} "
        f"csv_parse_mode={args['csv_parse_mode']} batch_id={batch_id} sources={sources}"
    )

    failures = []
    for source in sources:
        if source not in BRONZE_SOURCES:
            raise ValueError(f"Unknown source '{source}', expected one of {list(BRONZE_SOURCES.keys())}")
        load_id = default_load_id(source, args["load_mode"])
        try:
            ingest_source(
                spark, logger, source, BRONZE_SOURCES[source],
                args["raw_bucket"], args["warehouse_bucket"], args["bronze_database"],
                args["gold_database"], load_date, args["csv_parse_mode"],
                args["load_mode"], batch_id, load_id, force_reload,
            )
        except Exception as exc:  # noqa: BLE001 -- deliberately broad: one bad source must not block the rest
            logger.error(f"[{source}] FAILED: {exc}")
            failures.append(source)

    # Hand off this run's batch/date context to Silver and Gold via the
    # workflow's shared run-properties bag, so downstream jobs process
    # exactly the batch Bronze just landed without re-deriving it.
    if workflow_ctx and not failures:
        put_run_properties(workflow_ctx["workflow_name"], workflow_ctx["run_id"], {
            "batch_id": batch_id,
            "load_date": load_date,
            "load_mode": args["load_mode"],
        })

    job.commit()

    if failures:
        raise RuntimeError(f"bronze_job failed for sources: {failures}")


if __name__ == "__main__":
    main()
