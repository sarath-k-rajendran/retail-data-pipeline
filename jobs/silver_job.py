"""Silver layer Glue ETL job -- cleanse, conform, deduplicate, and upsert.

Reads the Bronze batch identified by batch_id (received from Bronze via the
Glue Workflow run-properties handoff, or passed explicitly for standalone
Console test runs), then for each source:
  1. Casts/standardizes types and timestamps to UTC.
  2. Deduplicates the incoming batch by business key, keeping one
     deterministic row per key (greatest (source_file, ingested_at)).
  3. Validates rows against a rule set; failing rows are quarantined with
     reason codes, passing rows continue.
  4. Joins reference dimensions (product/store/promotion) onto facts.
  5. Masks PII (customer_email -> salted hash) before it ever lands in Silver.
  6. MERGE INTO the conformed Iceberg table on business key:
     business_key exists in target -> UPDATE, else -> INSERT.
     No CDC/op_type columns are required (see plan.md "Simplified
     Incremental Logic") -- both --load_mode DELTA and SNAPSHOT follow the
     same upsert rule.

Produces: silver.dim_product, silver.dim_store, silver.dim_calendar,
silver.fact_order_lines, silver.fact_returns, plus silver.fact_web_sessions
and silver.fact_inventory_snapshot (needed to compute the Gold conversion,
stockout-rate, and inventory-turnover KPIs).

Idempotency: MERGE INTO is naturally idempotent on business_key -- rerunning
the same batch_id twice converges to the same end state rather than
duplicating rows (unlike Bronze's append, which needs an explicit guard).
"""
from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.dq_utils import (
    blank_check,
    date_order_check,
    dedup_keep_deterministic,
    enum_check,
    fk_check,
    null_check,
    range_check,
    validate,
)
from common.iceberg_utils import append_dataframe, create_table_if_not_exists, fqtn, merge_into
from common.logging_utils import RunMetrics, get_logger, persist_run_log
from common.schemas import SILVER_BUSINESS_KEYS
from common.secrets_utils import get_secret_string
from common.workflow_utils import get_run_properties, get_run_property, get_workflow_context

REQUIRED_ARGS = [
    "JOB_NAME",
    "warehouse_bucket",
    "bronze_database",
    "silver_database",
    "gold_database",
    "pii_salt_secret_name",
]
OPTIONAL_ARGS = {
    "batch_id": "",
    "load_date": "",
    "load_mode": "SNAPSHOT",
    "WORKFLOW_NAME": "",
    "WORKFLOW_RUN_ID": "",
}

DEDUP_TIEBREAK_COLS = ["source_file", "ingested_at"]
VALID_CHANNELS = ["STORE", "ONLINE"]
RETURN_REASON_CODES = ["DEFECTIVE", "WRONG_ITEM", "NOT_AS_DESCRIBED",
                        "CHANGED_MIND", "SIZE_ISSUE", "LATE_DELIVERY"]


def resolve_args(argv: list[str]) -> dict:
    all_arg_names = REQUIRED_ARGS + list(OPTIONAL_ARGS.keys())
    present = {a for a in all_arg_names if f"--{a}" in argv}
    resolved = getResolvedOptions(argv, list(present | set(REQUIRED_ARGS)))
    for key, default in OPTIONAL_ARGS.items():
        resolved.setdefault(key, default)
    return resolved


def resolve_batch_context(args: dict, logger) -> tuple[str, str, str]:
    """Determines (batch_id, load_date, load_mode) for this run: prefer the
    Glue Workflow run-properties handoff from Bronze; fall back to explicit
    job parameters for standalone testing."""
    ctx = get_workflow_context(args)
    if ctx:
        props = get_run_properties(ctx["workflow_name"], ctx["run_id"])
        batch_id = get_run_property(props, "batch_id") or args["batch_id"]
        load_date = get_run_property(props, "load_date") or args["load_date"]
        load_mode = get_run_property(props, "load_mode") or args["load_mode"]
        logger.info(f"Resolved batch context from workflow run properties: batch_id={batch_id}")
    else:
        batch_id, load_date, load_mode = args["batch_id"], args["load_date"], args["load_mode"]
        logger.info("No workflow context found -- using explicit job parameters (standalone run)")
    if not batch_id or not load_date:
        raise ValueError(
            "Could not resolve batch_id/load_date. Either run this job from the "
            "retail-mdp workflow (after bronze_job), or pass --batch_id and --load_date explicitly."
        )
    return batch_id, load_date, load_mode


def read_bronze_batch(spark, bronze_database: str, table: str, batch_id: str) -> DataFrame:
    return spark.table(fqtn(bronze_database, table)).filter(F.col("batch_id") == batch_id)


def quarantine_location(warehouse_bucket: str, table: str) -> str:
    return f"s3://{warehouse_bucket}/quarantine/{table}/"


def write_quarantine(spark, warehouse_bucket: str, silver_database: str, table: str,
                      quarantine_df: DataFrame, batch_id: str, load_id: str) -> int:
    if quarantine_df.rdd.isEmpty():
        return 0
    tagged = (
        quarantine_df
        .withColumn("quarantined_at", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("load_id", F.lit(load_id))
    )
    quarantine_table = f"{table}_rejects"
    schema_ddl = ", ".join(f"{f.name} {f.dataType.simpleString().upper()}" for f in tagged.schema.fields)
    create_table_if_not_exists(
        spark, silver_database, quarantine_table, schema_ddl,
        quarantine_location(warehouse_bucket, table), partition_clause="days(quarantined_at)",
    )
    append_dataframe(tagged, silver_database, quarantine_table)
    return tagged.count()


def mask_email(df: DataFrame, column: str, salt: str, out_column: str) -> DataFrame:
    return df.withColumn(out_column, F.sha2(F.concat(F.lit(salt), F.coalesce(F.col(column), F.lit(""))), 256)) \
              .drop(column)


def process_dim_product(spark, args, batch_id, load_id, logger) -> dict:
    bronze = read_bronze_batch(spark, args["bronze_database"], "product", batch_id)
    deduped, dropped = dedup_keep_deterministic(bronze, ["product_id"], DEDUP_TIEBREAK_COLS)

    rules = [
        null_check("product_id"),
        blank_check("product_name"),
        blank_check("category"),
        range_check("cost", min_val=0),
        range_check("list_price", min_val=0),
        enum_check("active_flag", ["Y", "N"]),
    ]
    valid_df, quarantine_df = validate(deduped, rules)

    conformed = (
        valid_df
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("category", F.trim(F.col("category")))
        .withColumn("is_active", F.col("active_flag") == F.lit("Y"))
        .withColumn("silver_updated_at", F.current_timestamp())
        .select("product_id", "product_name", "category", "sub_category", "brand",
                "cost", "list_price", "is_active", "created_at", "silver_updated_at")
    )

    location = f"s3://{args['warehouse_bucket']}/silver/dim_product/"
    schema_ddl = (
        "product_id STRING, product_name STRING, category STRING, sub_category STRING, "
        "brand STRING, cost DOUBLE, list_price DOUBLE, is_active BOOLEAN, "
        "created_at DATE, silver_updated_at TIMESTAMP"
    )
    create_table_if_not_exists(spark, args["silver_database"], "dim_product", schema_ddl, location)

    conformed.createOrReplaceTempView("src_dim_product")
    merge_into(
        spark, "src_dim_product", args["silver_database"], "dim_product",
        merge_keys=["product_id"],
        update_columns=["product_name", "category", "sub_category", "brand", "cost",
                         "list_price", "is_active", "created_at", "silver_updated_at"],
        insert_columns=["product_id", "product_name", "category", "sub_category", "brand",
                         "cost", "list_price", "is_active", "created_at", "silver_updated_at"],
    )

    rejected = write_quarantine(spark, args["warehouse_bucket"], args["silver_database"],
                                 "dim_product", quarantine_df, batch_id, load_id)
    return {"rows_read": bronze.count(), "rows_deduped_out": dropped,
            "rows_written": conformed.count(), "rows_rejected": rejected}


def process_dim_store(spark, args, batch_id, load_id, logger) -> dict:
    bronze = read_bronze_batch(spark, args["bronze_database"], "store", batch_id)
    deduped, dropped = dedup_keep_deterministic(bronze, ["store_id"], DEDUP_TIEBREAK_COLS)

    rules = [
        null_check("store_id"),
        blank_check("region"),
        blank_check("city"),
        enum_check("channel", VALID_CHANNELS),
    ]
    valid_df, quarantine_df = validate(deduped, rules)

    conformed = (
        valid_df
        .withColumn("region", F.trim(F.col("region")))
        .withColumn("silver_updated_at", F.current_timestamp())
        .select("store_id", "store_name", "region", "state", "city", "channel",
                "open_date", "silver_updated_at")
    )

    location = f"s3://{args['warehouse_bucket']}/silver/dim_store/"
    schema_ddl = (
        "store_id STRING, store_name STRING, region STRING, state STRING, city STRING, "
        "channel STRING, open_date DATE, silver_updated_at TIMESTAMP"
    )
    create_table_if_not_exists(spark, args["silver_database"], "dim_store", schema_ddl, location)

    conformed.createOrReplaceTempView("src_dim_store")
    merge_into(
        spark, "src_dim_store", args["silver_database"], "dim_store",
        merge_keys=["store_id"],
        update_columns=["store_name", "region", "state", "city", "channel", "open_date", "silver_updated_at"],
        insert_columns=["store_id", "store_name", "region", "state", "city", "channel",
                         "open_date", "silver_updated_at"],
    )

    rejected = write_quarantine(spark, args["warehouse_bucket"], args["silver_database"],
                                 "dim_store", quarantine_df, batch_id, load_id)
    return {"rows_read": bronze.count(), "rows_deduped_out": dropped,
            "rows_written": conformed.count(), "rows_rejected": rejected}


def ensure_dim_calendar(spark, args, logger) -> dict:
    """dim_calendar is generated, not sourced from a feed. Populated once
    (idempotent no-op on subsequent runs) covering a fixed wide range."""
    location = f"s3://{args['warehouse_bucket']}/silver/dim_calendar/"
    schema_ddl = (
        "calendar_date DATE, year INT, quarter INT, month INT, month_name STRING, "
        "day_of_month INT, day_of_week INT, day_name STRING, week_of_year INT, "
        "is_weekend BOOLEAN"
    )
    create_table_if_not_exists(spark, args["silver_database"], "dim_calendar", schema_ddl, location)

    existing_count = spark.table(fqtn(args["silver_database"], "dim_calendar")).limit(1).count()
    if existing_count > 0:
        logger.info("dim_calendar already populated -- skipping (idempotent)")
        return {"rows_written": 0, "skipped_idempotent": True}

    calendar_df = (
        spark.sql("SELECT explode(sequence(to_date('2020-01-01'), to_date('2035-12-31'), interval 1 day)) AS calendar_date")
        .withColumn("year", F.year("calendar_date"))
        .withColumn("quarter", F.quarter("calendar_date"))
        .withColumn("month", F.month("calendar_date"))
        .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
        .withColumn("day_of_month", F.dayofmonth("calendar_date"))
        .withColumn("day_of_week", F.dayofweek("calendar_date"))
        .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("calendar_date"))
        .withColumn("is_weekend", F.dayofweek("calendar_date").isin([1, 7]))
    )
    calendar_df.writeTo(fqtn(args["silver_database"], "dim_calendar")).append()
    return {"rows_written": calendar_df.count()}


def process_promotion_lookup(spark, args, batch_id, load_id, logger) -> DataFrame:
    """Validates/quarantines bronze.promotion and returns the valid rows,
    ready to join onto order lines. Not a standalone required Silver table
    (see module docstring) but still validated like every other bronze
    source before its attributes get denormalized onto fact_order_lines --
    otherwise a corrupt discount_pct or an inverted date range would flow
    straight through into the conformed fact table unfiltered."""
    promo_bronze = read_bronze_batch(spark, args["bronze_database"], "promotion", batch_id)
    promo_deduped, dropped = dedup_keep_deterministic(promo_bronze, ["promo_id"], DEDUP_TIEBREAK_COLS)

    rules = [
        null_check("promo_id"),
        range_check("discount_pct", min_val=0, max_val=100),
        date_order_check("start_date", "end_date"),
    ]
    promo_valid_df, promo_quarantine_df = validate(promo_deduped, rules)

    rejected = write_quarantine(spark, args["warehouse_bucket"], args["silver_database"],
                                 "promotion", promo_quarantine_df, batch_id, load_id)
    logger.info(f"[promotion] rows_read={promo_bronze.count()} rows_deduped_out={dropped} "
                f"rows_valid={promo_valid_df.count()} rows_rejected={rejected}")
    return promo_valid_df


def process_fact_order_lines(spark, args, batch_id, load_id, salt, logger) -> dict:
    bronze = read_bronze_batch(spark, args["bronze_database"], "orders", batch_id)
    deduped, dropped = dedup_keep_deterministic(bronze, ["order_id", "order_line_id"], DEDUP_TIEBREAK_COLS)
    # Normalize the "no promo" sentinel (blank string, per the source feed's
    # convention) to a real NULL before any rule/join treats promo_id as a
    # value to validate -- otherwise fk_check below would wrongly quarantine
    # every non-promo order line.
    deduped = deduped.withColumn(
        "promo_id",
        F.when(F.trim(F.coalesce(F.col("promo_id"), F.lit(""))) == "", F.lit(None).cast("string"))
         .otherwise(F.col("promo_id")),
    )

    promo_valid_df = process_promotion_lookup(spark, args, batch_id, load_id, logger)
    promo_ids = {r[0] for r in promo_valid_df.select("promo_id").distinct().collect()}
    product_ids = {r[0] for r in spark.table(fqtn(args["silver_database"], "dim_product"))
                   .select("product_id").distinct().collect()}
    store_ids = {r[0] for r in spark.table(fqtn(args["silver_database"], "dim_store"))
                 .select("store_id").distinct().collect()}

    rules = [
        null_check("order_id"),
        null_check("order_ts"),
        null_check("product_id"),
        null_check("store_id"),
        range_check("qty", min_val=1),
        range_check("unit_price", min_val=0),
        range_check("discount_amt", min_val=0),
        enum_check("channel", VALID_CHANNELS),
        fk_check("product_id", product_ids),
        fk_check("store_id", store_ids),
        fk_check("promo_id", promo_ids),
    ]
    valid_df, quarantine_df = validate(deduped, rules)

    # Denormalized promo attributes joined onto the order line (see module
    # docstring: promo isn't a standalone required Silver table, so its
    # relevant attributes are conformed here instead). Joined on BOTH
    # promo_id AND product_id -- a promo only applies to the product it was
    # actually defined for, so a promo_id that (through bad source data)
    # ended up on the wrong product's order line simply won't match here.
    promo_lookup = promo_valid_df.select(
        F.col("promo_id").alias("_promo_lookup_id"),
        F.col("promo_name"),
        F.col("discount_pct").alias("promo_discount_pct"),
        F.col("product_id").alias("_promo_lookup_product_id"),
    )

    dim_store = spark.table(fqtn(args["silver_database"], "dim_store")).select(
        "store_id", F.col("region").alias("store_region"), F.col("channel").alias("store_channel"),
    )
    dim_product = spark.table(fqtn(args["silver_database"], "dim_product")).select(
        "product_id", F.col("category").alias("product_category"),
        F.col("sub_category").alias("product_sub_category"),
    )

    enriched = (
        valid_df
        .join(
            promo_lookup,
            (F.col("promo_id") == F.col("_promo_lookup_id"))
            & (F.col("product_id") == F.col("_promo_lookup_product_id")),
            how="left",
        )
        .drop("_promo_lookup_id", "_promo_lookup_product_id")
        .join(dim_store, on="store_id", how="left")
        .join(dim_product, on="product_id", how="left")
        .withColumn("order_ts", F.to_utc_timestamp(F.col("order_ts"), "UTC"))
        .withColumn("net_amount", (F.col("qty") * F.col("unit_price")) - F.col("discount_amt"))
        .withColumn("has_promo", F.col("promo_id").isNotNull())
    )
    enriched = mask_email(enriched, "customer_email", salt, "customer_email_hash")

    conformed = enriched.select(
        "order_id", "order_line_id", "order_ts", "store_id", "store_region", "store_channel",
        "product_id", "product_category", "product_sub_category", "promo_id", "promo_name",
        "has_promo", "promo_discount_pct", "qty", "unit_price", "discount_amt", "net_amount",
        "channel", "customer_id", "customer_email_hash",
    ).withColumn("silver_updated_at", F.current_timestamp())

    location = f"s3://{args['warehouse_bucket']}/silver/fact_order_lines/"
    schema_ddl = (
        "order_id STRING, order_line_id INT, order_ts TIMESTAMP, store_id STRING, "
        "store_region STRING, store_channel STRING, product_id STRING, product_category STRING, "
        "product_sub_category STRING, promo_id STRING, promo_name STRING, has_promo BOOLEAN, "
        "promo_discount_pct INT, qty INT, unit_price DOUBLE, discount_amt DOUBLE, "
        "net_amount DOUBLE, channel STRING, customer_id STRING, customer_email_hash STRING, "
        "silver_updated_at TIMESTAMP"
    )
    create_table_if_not_exists(spark, args["silver_database"], "fact_order_lines", schema_ddl,
                                location, partition_clause="days(order_ts)")

    conformed.createOrReplaceTempView("src_fact_order_lines")
    merge_keys = SILVER_BUSINESS_KEYS["fact_order_lines"]
    update_cols = [c for c in conformed.columns if c not in merge_keys]
    merge_into(spark, "src_fact_order_lines", args["silver_database"], "fact_order_lines",
               merge_keys=merge_keys, update_columns=update_cols, insert_columns=conformed.columns)

    rejected = write_quarantine(spark, args["warehouse_bucket"], args["silver_database"],
                                 "fact_order_lines", quarantine_df, batch_id, load_id)
    return {"rows_read": bronze.count(), "rows_deduped_out": dropped,
            "rows_written": conformed.count(), "rows_rejected": rejected}


def process_fact_returns(spark, args, batch_id, load_id, logger) -> dict:
    bronze = read_bronze_batch(spark, args["bronze_database"], "returns", batch_id)
    deduped, dropped = dedup_keep_deterministic(bronze, ["return_id"], DEDUP_TIEBREAK_COLS)

    rules = [
        null_check("return_id"),
        null_check("order_id"),
        null_check("return_ts"),
        range_check("qty", min_val=1),
        enum_check("reason_code", RETURN_REASON_CODES),
    ]
    valid_df, quarantine_df = validate(deduped, rules)

    order_lines = spark.table(fqtn(args["silver_database"], "fact_order_lines")).select(
        "order_id", "order_line_id", "product_id", "product_category", "store_id", "store_region",
    )
    # Composite FK: order_id+order_line_id must exist in fact_order_lines.
    # A plain column-level rule can't express a multi-column relational
    # check, so it's done here as an explicit anti-join rather than forced
    # through the single-column dq_utils rule engine.
    matched = valid_df.join(order_lines, on=["order_id", "order_line_id"], how="inner")
    orphaned = valid_df.join(order_lines, on=["order_id", "order_line_id"], how="left_anti") \
                        .withColumn("reject_reasons", F.lit("ORPHAN_ORDER_LINE_FK"))

    conformed = (
        matched
        .withColumn("return_ts", F.to_utc_timestamp(F.col("return_ts"), "UTC"))
        .withColumn("silver_updated_at", F.current_timestamp())
        .select("return_id", "order_id", "order_line_id", "return_ts", "qty", "reason_code",
                "product_id", "product_category", "store_id", "store_region", "silver_updated_at")
    )

    location = f"s3://{args['warehouse_bucket']}/silver/fact_returns/"
    schema_ddl = (
        "return_id STRING, order_id STRING, order_line_id INT, return_ts TIMESTAMP, qty INT, "
        "reason_code STRING, product_id STRING, product_category STRING, store_id STRING, "
        "store_region STRING, silver_updated_at TIMESTAMP"
    )
    create_table_if_not_exists(spark, args["silver_database"], "fact_returns", schema_ddl,
                                location, partition_clause="days(return_ts)")

    conformed.createOrReplaceTempView("src_fact_returns")
    merge_keys = SILVER_BUSINESS_KEYS["fact_returns"]
    update_cols = [c for c in conformed.columns if c not in merge_keys]
    merge_into(spark, "src_fact_returns", args["silver_database"], "fact_returns",
               merge_keys=merge_keys, update_columns=update_cols, insert_columns=conformed.columns)

    quarantine_all = quarantine_df.unionByName(orphaned.select(quarantine_df.columns), allowMissingColumns=False) \
        if not orphaned.rdd.isEmpty() else quarantine_df
    rejected = write_quarantine(spark, args["warehouse_bucket"], args["silver_database"],
                                 "fact_returns", quarantine_all, batch_id, load_id)
    return {"rows_read": bronze.count(), "rows_deduped_out": dropped,
            "rows_written": conformed.count(), "rows_rejected": rejected}


def process_fact_web_sessions(spark, args, batch_id, load_id, logger) -> dict:
    bronze = read_bronze_batch(spark, args["bronze_database"], "web_sessions", batch_id)
    deduped, dropped = dedup_keep_deterministic(bronze, ["session_id"], DEDUP_TIEBREAK_COLS)

    rules = [
        null_check("session_id"),
        null_check("session_ts"),
        enum_check("channel", VALID_CHANNELS),
        enum_check("converted_flag", [0, 1]),
    ]
    valid_df, quarantine_df = validate(deduped, rules)

    conformed = (
        valid_df
        .withColumn("session_ts", F.to_utc_timestamp(F.col("session_ts"), "UTC"))
        .withColumn("is_converted", F.col("converted_flag") == F.lit(1))
        .withColumn("silver_updated_at", F.current_timestamp())
        .select("session_id", "customer_id", "session_ts", "store_id", "channel",
                "is_converted", "order_id", "silver_updated_at")
    )

    location = f"s3://{args['warehouse_bucket']}/silver/fact_web_sessions/"
    schema_ddl = (
        "session_id STRING, customer_id STRING, session_ts TIMESTAMP, store_id STRING, "
        "channel STRING, is_converted BOOLEAN, order_id STRING, silver_updated_at TIMESTAMP"
    )
    create_table_if_not_exists(spark, args["silver_database"], "fact_web_sessions", schema_ddl,
                                location, partition_clause="days(session_ts)")

    conformed.createOrReplaceTempView("src_fact_web_sessions")
    merge_keys = SILVER_BUSINESS_KEYS["fact_web_sessions"]
    update_cols = [c for c in conformed.columns if c not in merge_keys]
    merge_into(spark, "src_fact_web_sessions", args["silver_database"], "fact_web_sessions",
               merge_keys=merge_keys, update_columns=update_cols, insert_columns=conformed.columns)

    rejected = write_quarantine(spark, args["warehouse_bucket"], args["silver_database"],
                                 "fact_web_sessions", quarantine_df, batch_id, load_id)
    return {"rows_read": bronze.count(), "rows_deduped_out": dropped,
            "rows_written": conformed.count(), "rows_rejected": rejected}


def process_fact_inventory_snapshot(spark, args, batch_id, load_id, logger) -> dict:
    bronze = read_bronze_batch(spark, args["bronze_database"], "inventory_snapshot", batch_id)
    deduped, dropped = dedup_keep_deterministic(
        bronze, ["store_id", "product_id", "snapshot_date"], DEDUP_TIEBREAK_COLS
    )

    product_ids = {r[0] for r in spark.table(fqtn(args["silver_database"], "dim_product"))
                   .select("product_id").distinct().collect()}
    store_ids = {r[0] for r in spark.table(fqtn(args["silver_database"], "dim_store"))
                 .select("store_id").distinct().collect()}

    rules = [
        null_check("store_id"),
        null_check("product_id"),
        null_check("snapshot_date"),
        range_check("on_hand_qty", min_val=0),
        range_check("on_order_qty", min_val=0),
        fk_check("product_id", product_ids),
        fk_check("store_id", store_ids),
    ]
    valid_df, quarantine_df = validate(deduped, rules)

    conformed = (
        valid_df
        .withColumn("is_stockout", F.col("on_hand_qty") == 0)
        .withColumn("silver_updated_at", F.current_timestamp())
        .select("store_id", "product_id", "snapshot_date", "on_hand_qty", "on_order_qty",
                "is_stockout", "silver_updated_at")
    )

    location = f"s3://{args['warehouse_bucket']}/silver/fact_inventory_snapshot/"
    schema_ddl = (
        "store_id STRING, product_id STRING, snapshot_date DATE, on_hand_qty INT, "
        "on_order_qty INT, is_stockout BOOLEAN, silver_updated_at TIMESTAMP"
    )
    create_table_if_not_exists(spark, args["silver_database"], "fact_inventory_snapshot", schema_ddl,
                                location, partition_clause="days(snapshot_date)")

    conformed.createOrReplaceTempView("src_fact_inventory_snapshot")
    merge_keys = SILVER_BUSINESS_KEYS["fact_inventory_snapshot"]
    update_cols = [c for c in conformed.columns if c not in merge_keys]
    merge_into(spark, "src_fact_inventory_snapshot", args["silver_database"], "fact_inventory_snapshot",
               merge_keys=merge_keys, update_columns=update_cols, insert_columns=conformed.columns)

    rejected = write_quarantine(spark, args["warehouse_bucket"], args["silver_database"],
                                 "fact_inventory_snapshot", quarantine_df, batch_id, load_id)
    return {"rows_read": bronze.count(), "rows_deduped_out": dropped,
            "rows_written": conformed.count(), "rows_rejected": rejected}


def main() -> None:
    args = resolve_args(sys.argv)
    logger = get_logger(args["JOB_NAME"])

    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    batch_id, load_date, load_mode = resolve_batch_context(args, logger)
    load_id = f"silver_{load_mode}_{batch_id[:12]}"
    salt = get_secret_string(args["pii_salt_secret_name"], key="salt")

    logger.info(f"Starting silver_job: batch_id={batch_id} load_date={load_date} load_mode={load_mode}")

    # Order matters: dims must land before facts (facts FK-validate + join
    # against them); fact_returns must land after fact_order_lines
    # (composite FK check); fact_order_lines needs dims for enrichment.
    steps = [
        ("dim_product", lambda: process_dim_product(spark, args, batch_id, load_id, logger)),
        ("dim_store", lambda: process_dim_store(spark, args, batch_id, load_id, logger)),
        ("dim_calendar", lambda: ensure_dim_calendar(spark, args, logger)),
        ("fact_order_lines", lambda: process_fact_order_lines(spark, args, batch_id, load_id, salt, logger)),
        ("fact_returns", lambda: process_fact_returns(spark, args, batch_id, load_id, logger)),
        ("fact_web_sessions", lambda: process_fact_web_sessions(spark, args, batch_id, load_id, logger)),
        ("fact_inventory_snapshot", lambda: process_fact_inventory_snapshot(spark, args, batch_id, load_id, logger)),
    ]

    failures = []
    for table, step_fn in steps:
        metrics = RunMetrics("silver_job", "silver", table, batch_id, load_id)
        metrics.set("watermark", load_date)
        try:
            result = step_fn()
            for k, v in result.items():
                metrics.set(k, v)
            record = metrics.log(logger)
        except Exception as exc:  # noqa: BLE001 -- one table's failure must not block independent tables
            record = metrics.log(logger, status="FAILED", error=str(exc))
            logger.error(f"[{table}] FAILED: {exc}")
            failures.append(table)
            continue
        persist_run_log(spark, args["gold_database"], f"s3://{args['warehouse_bucket']}/gold", record)

    job.commit()

    if failures:
        raise RuntimeError(f"silver_job failed for tables: {failures}")


if __name__ == "__main__":
    main()
