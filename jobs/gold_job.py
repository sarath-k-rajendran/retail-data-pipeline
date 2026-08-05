"""Gold layer Glue ETL job -- retail KPI marts.

Design principle: every Gold table is a full recompute, from CURRENT-STATE
Silver tables, of a bounded date window (not an incremental aggregation of
"new rows since last run"). This is what makes Gold idempotent and
backfill-safe: rerunning for the same window with the same Silver contents
always produces the same output rows, and late-arriving facts (e.g. a return
that lands today against an order from last week) are correctly reflected
the next time that order's date falls inside the recompute window, because
we always re-aggregate the full current truth for that window rather than
patching in a delta.

Recompute window: by default, the trailing `--recompute_window_days` days
(default 3) ending at load_date -- wide enough to absorb the kind of
late corrections the mock generator's delta mode produces (next-day
corrections to yesterday's order lines), while bounding compute cost versus
a full-history rescan every run. For a genuine historical backfill, pass
--backfill_start_date/--backfill_end_date explicitly to override the window.

kpi_promo_lift is a different shape: lift is fundamentally a comparison
across many days (a single day/product is either "on promo" or "not," so a
single day can't show lift on its own), so it's computed as a point-in-time
snapshot over a trailing --promo_lift_window_days (default 90) history,
written as one row per (region, category, channel) tagged with kpi_date =
load_date -- i.e. "as computed on load_date, using the last 90 days."

Tables produced (all partitioned by days(kpi_date) per plan.md):
  gold.kpi_sales_daily     -- Gross/Net Sales, Discount %, Gross Margin %, AOV, Units/Transaction
  gold.kpi_returns_daily   -- Return Rate % (cohort-based: attributed to the ORIGINAL order date)
  gold.kpi_inventory_daily -- Stockout Rate %, Inventory Turnover (daily proxy, see formula comment)
  gold.kpi_promo_lift      -- Promo Lift % (trailing-window units comparison)
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from common.dq_report import run_all as run_all_dq_checks
from common.iceberg_utils import create_table_if_not_exists, fqtn, overwrite_partitions
from common.logging_utils import RunMetrics, get_logger, persist_run_log
from common.workflow_utils import get_run_properties, get_workflow_context

REQUIRED_ARGS = ["JOB_NAME", "warehouse_bucket", "silver_database", "gold_database"]
OPTIONAL_ARGS = {
    "load_date": "",
    "recompute_window_days": "3",
    "promo_lift_window_days": "90",
    "backfill_start_date": "",
    "backfill_end_date": "",
    "run_dq_checks": "false",
    "WORKFLOW_NAME": "",
    "WORKFLOW_RUN_ID": "",
}


def resolve_args(argv: list[str]) -> dict:
    all_arg_names = REQUIRED_ARGS + list(OPTIONAL_ARGS.keys())
    present = {a for a in all_arg_names if f"--{a}" in argv}
    resolved = getResolvedOptions(argv, list(present | set(REQUIRED_ARGS)))
    for key, default in OPTIONAL_ARGS.items():
        resolved.setdefault(key, default)
    return resolved


def resolve_recompute_range(args: dict, logger) -> tuple[date, date, date]:
    ctx = get_workflow_context(args)
    load_date_str = args["load_date"]
    if ctx:
        props = get_run_properties(ctx["workflow_name"], ctx["run_id"])
        load_date_str = props.get("load_date") or load_date_str
        logger.info(f"Resolved load_date from workflow run properties: {load_date_str}")
    if not load_date_str:
        raise ValueError(
            "Could not resolve load_date. Either run this job from the retail-mdp "
            "workflow (after bronze_job/silver_job), or pass --load_date explicitly."
        )
    load_date = datetime.strptime(load_date_str, "%Y-%m-%d").date()

    if args["backfill_start_date"] and args["backfill_end_date"]:
        start = datetime.strptime(args["backfill_start_date"], "%Y-%m-%d").date()
        end = datetime.strptime(args["backfill_end_date"], "%Y-%m-%d").date()
    else:
        window = int(args["recompute_window_days"])
        end = load_date
        start = load_date - timedelta(days=window - 1)
    return start, end, load_date


def date_between(column: str, start: date, end: date):
    return (F.col(column) >= F.to_date(F.lit(str(start)))) & (F.col(column) <= F.to_date(F.lit(str(end))))


def compute_kpi_sales_daily(spark, args: dict, start: date, end: date) -> dict:
    fact = (
        spark.table(fqtn(args["silver_database"], "fact_order_lines"))
        .withColumn("kpi_date", F.to_date("order_ts"))
        .filter(date_between("kpi_date", start, end))
    )
    dim_product_cost = spark.table(fqtn(args["silver_database"], "dim_product")) \
        .select("product_id", F.col("cost").alias("unit_cost"))
    fact = fact.join(dim_product_cost, on="product_id", how="left")

    agg = (
        fact.groupBy("kpi_date", "store_region", "product_category", "channel")
        .agg(
            F.sum(F.col("qty") * F.col("unit_price")).alias("gross_sales"),
            F.sum("discount_amt").alias("discount_amount"),
            F.sum("net_amount").alias("net_sales"),
            F.sum(F.col("qty") * F.coalesce(F.col("unit_cost"), F.lit(0.0))).alias("total_cost"),
            F.sum("qty").alias("units"),
            F.countDistinct("order_id").alias("order_count"),
        )
        .withColumnRenamed("store_region", "region")
        .withColumnRenamed("product_category", "category")
    )

    # discount_pct     = discount_amount / gross_sales
    # gross_margin_pct = (net_sales - total_cost) / net_sales   (cost measured against net, i.e. actual realized revenue)
    # aov               = net_sales / distinct order count in this grain
    #                     NOTE: since orders can span multiple categories, an order's net sales
    #                     are naturally split across each category it touches -- aov here reads
    #                     as "average value this category/region/channel slice contributed to the
    #                     orders that included it," not a whole-order AOV. Documented deliberately.
    # units_per_transaction = units / distinct order count in this grain
    kpis = (
        agg
        .withColumn("discount_pct", F.when(F.col("gross_sales") > 0, F.col("discount_amount") / F.col("gross_sales") * 100).otherwise(F.lit(0.0)))
        .withColumn("gross_margin", F.col("net_sales") - F.col("total_cost"))
        .withColumn("gross_margin_pct", F.when(F.col("net_sales") > 0, (F.col("net_sales") - F.col("total_cost")) / F.col("net_sales") * 100).otherwise(F.lit(0.0)))
        .withColumn("aov", F.when(F.col("order_count") > 0, F.col("net_sales") / F.col("order_count")).otherwise(F.lit(0.0)))
        .withColumn("units_per_transaction", F.when(F.col("order_count") > 0, F.col("units") / F.col("order_count")).otherwise(F.lit(0.0)))
        .withColumn("computed_at", F.current_timestamp())
        .select("kpi_date", "region", "category", "channel", "gross_sales", "discount_amount",
                "net_sales", "discount_pct", "total_cost", "gross_margin", "gross_margin_pct",
                "order_count", "units", "aov", "units_per_transaction", "computed_at")
    )

    location = f"s3://{args['warehouse_bucket']}/gold/kpi_sales_daily/"
    schema_ddl = (
        "kpi_date DATE, region STRING, category STRING, channel STRING, gross_sales DOUBLE, "
        "discount_amount DOUBLE, net_sales DOUBLE, discount_pct DOUBLE, total_cost DOUBLE, "
        "gross_margin DOUBLE, gross_margin_pct DOUBLE, order_count BIGINT, units BIGINT, "
        "aov DOUBLE, units_per_transaction DOUBLE, computed_at TIMESTAMP"
    )
    create_table_if_not_exists(spark, args["gold_database"], "kpi_sales_daily", schema_ddl,
                                location, partition_clause="days(kpi_date)")
    overwrite_partitions(kpis, args["gold_database"], "kpi_sales_daily")
    return {"rows_written": kpis.count()}


def compute_kpi_returns_daily(spark, args: dict, start: date, end: date) -> dict:
    order_lines = (
        spark.table(fqtn(args["silver_database"], "fact_order_lines"))
        .withColumn("kpi_date", F.to_date("order_ts"))
        .filter(date_between("kpi_date", start, end))
        .select("order_id", "order_line_id", "kpi_date", "store_region", "product_category", "channel", "qty")
    )
    # Cohort-based: a return is attributed to the KPI date of the ORIGINAL
    # order, not the date the return itself was processed -- this measures
    # "of what we sold on day D, what fraction was eventually returned,"
    # which is the standard retail return-rate definition. Because we don't
    # date-filter fact_returns itself, a return that arrives weeks after the
    # sale is still correctly folded into its original order date's KPI row
    # the next time that date falls inside the recompute window.
    returns = spark.table(fqtn(args["silver_database"], "fact_returns")) \
        .select("order_id", "order_line_id", F.col("qty").alias("returned_qty"))

    joined = order_lines.join(returns, on=["order_id", "order_line_id"], how="left") \
        .withColumn("returned_qty", F.coalesce(F.col("returned_qty"), F.lit(0)))

    agg = (
        joined.groupBy("kpi_date", "store_region", "product_category", "channel")
        .agg(F.sum("qty").alias("units_sold"), F.sum("returned_qty").alias("units_returned"))
        .withColumnRenamed("store_region", "region")
        .withColumnRenamed("product_category", "category")
        .withColumn("return_rate_pct", F.when(F.col("units_sold") > 0, F.col("units_returned") / F.col("units_sold") * 100).otherwise(F.lit(0.0)))
        .withColumn("computed_at", F.current_timestamp())
        .select("kpi_date", "region", "category", "channel", "units_sold", "units_returned",
                "return_rate_pct", "computed_at")
    )

    location = f"s3://{args['warehouse_bucket']}/gold/kpi_returns_daily/"
    schema_ddl = (
        "kpi_date DATE, region STRING, category STRING, channel STRING, units_sold BIGINT, "
        "units_returned BIGINT, return_rate_pct DOUBLE, computed_at TIMESTAMP"
    )
    create_table_if_not_exists(spark, args["gold_database"], "kpi_returns_daily", schema_ddl,
                                location, partition_clause="days(kpi_date)")
    overwrite_partitions(agg, args["gold_database"], "kpi_returns_daily")
    return {"rows_written": agg.count()}


def compute_kpi_inventory_daily(spark, args: dict, start: date, end: date) -> dict:
    snapshot = spark.table(fqtn(args["silver_database"], "fact_inventory_snapshot")) \
        .filter(date_between("snapshot_date", start, end))
    dim_store = spark.table(fqtn(args["silver_database"], "dim_store")).select("store_id", "region", "channel")
    dim_product = spark.table(fqtn(args["silver_database"], "dim_product")).select("product_id", "category")

    enriched = snapshot.join(dim_store, "store_id", "left").join(dim_product, "product_id", "left")

    inventory_agg = (
        enriched.groupBy(F.col("snapshot_date").alias("kpi_date"), "region", "category", "channel")
        .agg(
            F.count(F.lit(1)).alias("sku_store_count"),
            F.sum(F.when(F.col("is_stockout"), 1).otherwise(0)).alias("stockout_count"),
            F.avg("on_hand_qty").alias("avg_on_hand_qty"),
        )
        .withColumn("stockout_rate_pct", F.when(F.col("sku_store_count") > 0, F.col("stockout_count") / F.col("sku_store_count") * 100).otherwise(F.lit(0.0)))
    )

    sales = (
        spark.table(fqtn(args["silver_database"], "fact_order_lines"))
        .withColumn("kpi_date", F.to_date("order_ts"))
        .filter(date_between("kpi_date", start, end))
        .groupBy("kpi_date", F.col("store_region").alias("region"), F.col("product_category").alias("category"), "channel")
        .agg(F.sum("qty").alias("units_sold"))
    )

    # inventory_turnover: a DAILY proxy (units sold that day / average on-hand
    # units that day), not the classical annualized COGS/avg-inventory ratio
    # -- documented explicitly since "turnover" computed daily is an unusual
    # grain; multiply by 365 outside this table for an annualized estimate.
    combined = (
        inventory_agg.join(sales, on=["kpi_date", "region", "category", "channel"], how="left")
        .withColumn("units_sold", F.coalesce(F.col("units_sold"), F.lit(0)))
        .withColumn("inventory_turnover", F.when(F.col("avg_on_hand_qty") > 0, F.col("units_sold") / F.col("avg_on_hand_qty")).otherwise(F.lit(0.0)))
        .withColumn("computed_at", F.current_timestamp())
        .select("kpi_date", "region", "category", "channel", "sku_store_count", "stockout_count",
                "stockout_rate_pct", "avg_on_hand_qty", "units_sold", "inventory_turnover", "computed_at")
    )

    location = f"s3://{args['warehouse_bucket']}/gold/kpi_inventory_daily/"
    schema_ddl = (
        "kpi_date DATE, region STRING, category STRING, channel STRING, sku_store_count BIGINT, "
        "stockout_count BIGINT, stockout_rate_pct DOUBLE, avg_on_hand_qty DOUBLE, "
        "units_sold BIGINT, inventory_turnover DOUBLE, computed_at TIMESTAMP"
    )
    create_table_if_not_exists(spark, args["gold_database"], "kpi_inventory_daily", schema_ddl,
                                location, partition_clause="days(kpi_date)")
    overwrite_partitions(combined, args["gold_database"], "kpi_inventory_daily")
    return {"rows_written": combined.count()}


def compute_kpi_promo_lift(spark, args: dict, load_date: date, window_days: int) -> dict:
    window_start = load_date - timedelta(days=window_days - 1)
    fact = (
        spark.table(fqtn(args["silver_database"], "fact_order_lines"))
        .withColumn("order_date", F.to_date("order_ts"))
        .filter(date_between("order_date", window_start, load_date))
    )

    per_day = (
        fact.groupBy("store_region", "product_category", "channel", "order_date", "has_promo")
        .agg(F.sum("qty").alias("units"))
    )

    promo_avg = (
        per_day.filter(F.col("has_promo"))
        .groupBy("store_region", "product_category", "channel")
        .agg(F.avg("units").alias("avg_units_promo_day"))
    )
    non_promo_avg = (
        per_day.filter(~F.col("has_promo"))
        .groupBy("store_region", "product_category", "channel")
        .agg(F.avg("units").alias("avg_units_non_promo_day"))
    )

    # promo_lift_pct = (avg daily units sold ON promo - avg daily units sold
    # OFF promo) / avg daily units sold OFF promo, for the same
    # region/category/channel slice over the trailing window -- a standard
    # volume-lift definition. NULL where there's no non-promo baseline to
    # compare against (division by zero would be meaningless, not zero).
    lift = (
        promo_avg.join(non_promo_avg, on=["store_region", "product_category", "channel"], how="outer")
        .withColumnRenamed("store_region", "region")
        .withColumnRenamed("product_category", "category")
        .withColumn("avg_units_promo_day", F.coalesce(F.col("avg_units_promo_day"), F.lit(0.0)))
        .withColumn("avg_units_non_promo_day", F.coalesce(F.col("avg_units_non_promo_day"), F.lit(0.0)))
        .withColumn(
            "promo_lift_pct",
            F.when(F.col("avg_units_non_promo_day") > 0,
                   (F.col("avg_units_promo_day") - F.col("avg_units_non_promo_day")) / F.col("avg_units_non_promo_day") * 100)
             .otherwise(F.lit(None).cast("double")),
        )
        .withColumn("kpi_date", F.to_date(F.lit(str(load_date))))
        .withColumn("window_days", F.lit(window_days))
        .withColumn("computed_at", F.current_timestamp())
        .select("kpi_date", "region", "category", "channel", "avg_units_promo_day",
                "avg_units_non_promo_day", "promo_lift_pct", "window_days", "computed_at")
    )

    location = f"s3://{args['warehouse_bucket']}/gold/kpi_promo_lift/"
    schema_ddl = (
        "kpi_date DATE, region STRING, category STRING, channel STRING, "
        "avg_units_promo_day DOUBLE, avg_units_non_promo_day DOUBLE, promo_lift_pct DOUBLE, "
        "window_days INT, computed_at TIMESTAMP"
    )
    create_table_if_not_exists(spark, args["gold_database"], "kpi_promo_lift", schema_ddl,
                                location, partition_clause="days(kpi_date)")
    overwrite_partitions(lift, args["gold_database"], "kpi_promo_lift")
    return {"rows_written": lift.count()}


def main() -> None:
    args = resolve_args(sys.argv)
    logger = get_logger(args["JOB_NAME"])

    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    start, end, load_date = resolve_recompute_range(args, logger)
    promo_window_days = int(args["promo_lift_window_days"])

    logger.info(
        f"Starting gold_job: recompute_window=[{start},{end}] load_date={load_date} "
        f"promo_lift_window_days={promo_window_days}"
    )

    steps = [
        ("kpi_sales_daily", lambda: compute_kpi_sales_daily(spark, args, start, end)),
        ("kpi_returns_daily", lambda: compute_kpi_returns_daily(spark, args, start, end)),
        ("kpi_inventory_daily", lambda: compute_kpi_inventory_daily(spark, args, start, end)),
        ("kpi_promo_lift", lambda: compute_kpi_promo_lift(spark, args, load_date, promo_window_days)),
    ]

    failures = []
    for table, step_fn in steps:
        metrics = RunMetrics("gold_job", "gold", table, batch_id="n/a", load_id=f"gold_{load_date}")
        metrics.set("watermark", str(load_date))
        try:
            result = step_fn()
            for k, v in result.items():
                metrics.set(k, v)
            record = metrics.log(logger)
        except Exception as exc:  # noqa: BLE001 -- one KPI mart's failure must not block independent marts
            record = metrics.log(logger, status="FAILED", error=str(exc))
            logger.error(f"[{table}] FAILED: {exc}")
            failures.append(table)
            continue
        persist_run_log(spark, args["gold_database"], f"s3://{args['warehouse_bucket']}/gold", record)

    # Optional inline DQ pass, reusing this job's already-running Spark
    # session -- no extra Glue job or Interactive Session needed (both may
    # be unavailable under a restricted job-count/DPU/session budget). Off
    # by default to keep the common-path run cheap; enable per-run with
    # --run_dq_checks true. Findings are logged, not fatal: a DQ finding
    # here is informational feedback on already-published KPIs, not a
    # reason to fail a gold_job run that otherwise succeeded.
    if args["run_dq_checks"].strip().lower() == "true":
        dq_failures = run_all_dq_checks(spark, args["silver_database"], args["gold_database"], log_fn=logger.info)
        if dq_failures:
            logger.warning(f"DQ check pass found {len(dq_failures)} failing check(s) -- see report above")

    job.commit()

    if failures:
        raise RuntimeError(f"gold_job failed for tables: {failures}")


if __name__ == "__main__":
    main()
