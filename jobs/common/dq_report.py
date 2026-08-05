"""Schema-aware, full-layer data-quality report for the retail Medallion
pipeline, built on top of the generic rule primitives in dq_utils.py.

Lives in jobs/common/ (shipped to every job via common.zip / --extra-py-files)
specifically so it can be invoked WITHOUT needing Glue Interactive Sessions or
a dedicated fourth Glue job -- both of which may be unavailable in
capacity-restricted accounts (limited job count / DPU budget / sessions
disabled). Two ways to run it, neither requiring a session:

  1. Inline, inside gold_job.py, by passing --run_dq_checks true (see
     gold_job.py's main()) -- reuses that job's already-running Spark
     session, so it costs no additional DPU allocation of its own.
  2. Standalone via tests/data_quality_checks.py, which is a thin CLI
     wrapper around run_all() below -- usable as its own Glue Python job if
     your environment has spare job-count/DPU budget, or via Athena SQL
     instead (see README.md Sec. 6) if it doesn't.
"""
from __future__ import annotations

from common.iceberg_utils import fqtn
from common.dq_utils import duplicate_count, null_rate, orphan_count
from common.schemas import SILVER_BUSINESS_KEYS


class CheckResult:
    def __init__(self, table: str, name: str, passed: bool, detail: str):
        self.table = table
        self.name = name
        self.passed = passed
        self.detail = detail

    def __repr__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.table}: {self.name} -- {self.detail}"


def _completeness_check(spark, database: str, table: str, column: str, results: list) -> None:
    df = spark.table(fqtn(database, table))
    rate = null_rate(df, column)
    passed = rate == 0.0
    results.append(CheckResult(table, f"completeness[{column}]", passed, f"null_rate={rate:.4f}"))


def _uniqueness_check(spark, database: str, table: str, keys: list[str], results: list) -> None:
    df = spark.table(fqtn(database, table))
    dupes = duplicate_count(df, keys)
    passed = dupes == 0
    results.append(CheckResult(table, f"uniqueness{keys}", passed, f"duplicate_key_count={dupes}"))


def _fk_check(spark, database: str, table: str, fk_column: str, ref_database: str,
              ref_table: str, ref_column: str, results: list) -> None:
    df = spark.table(fqtn(database, table))
    ref_df = spark.table(fqtn(ref_database, ref_table))
    orphans = orphan_count(df, fk_column, ref_df, ref_column)
    passed = orphans == 0
    results.append(CheckResult(
        table, f"referential_integrity[{fk_column}->{ref_table}.{ref_column}]",
        passed, f"orphan_count={orphans}",
    ))


def _composite_fk_check(spark, database: str, table: str, keys: list[str],
                         ref_database: str, ref_table: str, results: list) -> None:
    """Multi-column FK check (e.g. (order_id, order_line_id) -> fact_order_lines),
    done as a plain anti-join since dq_utils' orphan_count is single-column only."""
    df = spark.table(fqtn(database, table))
    ref_df = spark.table(fqtn(ref_database, ref_table)).select(*keys).distinct()
    orphans = df.join(ref_df, on=keys, how="left_anti").count()
    passed = orphans == 0
    results.append(CheckResult(
        table, f"referential_integrity{keys}->{ref_table}", passed, f"orphan_count={orphans}",
    ))


def _row_count_check(spark, database: str, table: str, results: list, min_rows: int = 1) -> None:
    df = spark.table(fqtn(database, table))
    count = df.count()
    passed = count >= min_rows
    results.append(CheckResult(table, "row_count", passed, f"row_count={count} (min_expected={min_rows})"))


def _range_sanity_check(spark, database: str, table: str, column: str, min_val: float,
                         max_val: float, results: list) -> None:
    from pyspark.sql import functions as F
    df = spark.table(fqtn(database, table))
    violations = df.filter((F.col(column) < min_val) | (F.col(column) > max_val)).count()
    passed = violations == 0
    results.append(CheckResult(
        table, f"range_sanity[{column} in [{min_val},{max_val}]]", passed, f"violations={violations}",
    ))


def check_silver_layer(spark, silver_database: str) -> list[CheckResult]:
    results: list[CheckResult] = []

    _completeness_check(spark, silver_database, "dim_product", "product_id", results)
    _uniqueness_check(spark, silver_database, "dim_product", SILVER_BUSINESS_KEYS["dim_product"], results)

    _completeness_check(spark, silver_database, "dim_store", "store_id", results)
    _uniqueness_check(spark, silver_database, "dim_store", SILVER_BUSINESS_KEYS["dim_store"], results)

    _completeness_check(spark, silver_database, "fact_order_lines", "order_id", results)
    _uniqueness_check(spark, silver_database, "fact_order_lines", SILVER_BUSINESS_KEYS["fact_order_lines"], results)
    _fk_check(spark, silver_database, "fact_order_lines", "product_id", silver_database, "dim_product", "product_id", results)
    _fk_check(spark, silver_database, "fact_order_lines", "store_id", silver_database, "dim_store", "store_id", results)

    _completeness_check(spark, silver_database, "fact_returns", "return_id", results)
    _uniqueness_check(spark, silver_database, "fact_returns", SILVER_BUSINESS_KEYS["fact_returns"], results)
    _composite_fk_check(spark, silver_database, "fact_returns", ["order_id", "order_line_id"],
                         silver_database, "fact_order_lines", results)

    _completeness_check(spark, silver_database, "fact_web_sessions", "session_id", results)
    _uniqueness_check(spark, silver_database, "fact_web_sessions", SILVER_BUSINESS_KEYS["fact_web_sessions"], results)

    _completeness_check(spark, silver_database, "fact_inventory_snapshot", "store_id", results)
    _uniqueness_check(spark, silver_database, "fact_inventory_snapshot",
                       SILVER_BUSINESS_KEYS["fact_inventory_snapshot"], results)
    _fk_check(spark, silver_database, "fact_inventory_snapshot", "product_id", silver_database, "dim_product", "product_id", results)
    _fk_check(spark, silver_database, "fact_inventory_snapshot", "store_id", silver_database, "dim_store", "store_id", results)

    _completeness_check(spark, silver_database, "dim_calendar", "calendar_date", results)
    _row_count_check(spark, silver_database, "dim_calendar", results, min_rows=1000)

    return results


def check_gold_layer(spark, gold_database: str) -> list[CheckResult]:
    results: list[CheckResult] = []

    for table in ["kpi_sales_daily", "kpi_returns_daily", "kpi_inventory_daily", "kpi_promo_lift"]:
        _completeness_check(spark, gold_database, table, "kpi_date", results)
        _row_count_check(spark, gold_database, table, results, min_rows=1)

    _range_sanity_check(spark, gold_database, "kpi_sales_daily", "discount_pct", 0, 100, results)
    _range_sanity_check(spark, gold_database, "kpi_returns_daily", "return_rate_pct", 0, 100, results)
    _range_sanity_check(spark, gold_database, "kpi_inventory_daily", "stockout_rate_pct", 0, 100, results)

    from pyspark.sql import functions as F
    sales = spark.table(fqtn(gold_database, "kpi_sales_daily"))
    # net_sales = gross_sales - discount_amount, and discount_amount >= 0
    # (enforced by Silver's range_check on discount_amt) -- so net must never
    # exceed gross, beyond floating-point rounding noise.
    violations = sales.filter(F.col("net_sales") > F.col("gross_sales") + 0.01).count()
    results.append(CheckResult(
        "kpi_sales_daily", "invariant[net_sales<=gross_sales]", violations == 0, f"violations={violations}",
    ))

    return results


def run_all(spark, silver_database: str = "retail_silver", gold_database: str = "retail_gold",
            log_fn=print) -> list[CheckResult]:
    results = check_silver_layer(spark, silver_database) + check_gold_layer(spark, gold_database)
    failures = [r for r in results if not r.passed]

    log_fn(f"=== Data Quality Report: {len(results)} checks, {len(failures)} failed ===")
    for r in results:
        log_fn(str(r))
    log_fn("=" * 60)

    return failures
