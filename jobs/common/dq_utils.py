"""Reusable data-quality check library.

Used two ways:
1. Inside Silver (silver_job.py): dedup + rule-based validation/quarantine
   with reason codes, before the conformed MERGE.
2. Standalone, post-hoc, against already-written Iceberg tables (see
   tests/data_quality_checks.py): completeness/uniqueness/referential-integrity
   assertions run after a real pipeline execution, via Athena or a Glue
   interactive session -- same functions, different caller.

Design: a "rule" is a dict {"reason_code": str, "condition": pyspark.Column}
where `condition` evaluates to True for a BAD row. `validate()` runs every
rule against the input DataFrame and splits it into (valid_df, quarantine_df)
based on whether any rule fired, recording every reason that fired per row.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

REJECT_REASONS_COL = "reject_reasons"


# --------------------------------------------------------------------------
# Rule builders
# --------------------------------------------------------------------------

def null_check(column: str, reason_code: str | None = None) -> dict:
    return {"reason_code": reason_code or f"NULL_{column.upper()}", "condition": F.col(column).isNull()}


def blank_check(column: str, reason_code: str | None = None) -> dict:
    return {
        "reason_code": reason_code or f"BLANK_{column.upper()}",
        "condition": F.col(column).isNull() | (F.trim(F.col(column)) == ""),
    }


def range_check(column: str, min_val=None, max_val=None, reason_code: str | None = None) -> dict:
    cond = F.lit(False)
    if min_val is not None:
        cond = cond | (F.col(column).isNotNull() & (F.col(column) < min_val))
    if max_val is not None:
        cond = cond | (F.col(column).isNotNull() & (F.col(column) > max_val))
    return {"reason_code": reason_code or f"OUT_OF_RANGE_{column.upper()}", "condition": cond}


def enum_check(column: str, allowed_values: list, reason_code: str | None = None) -> dict:
    return {
        "reason_code": reason_code or f"INVALID_ENUM_{column.upper()}",
        "condition": F.col(column).isNotNull() & (~F.col(column).isin(allowed_values)),
    }


def fk_check(column: str, valid_keys: set, reason_code: str | None = None) -> dict:
    """Flags non-null values that don't resolve against a reference key set.
    Null FKs are intentionally not flagged here -- pair with null_check if the
    FK is also required."""
    keys_list = list(valid_keys)
    return {
        "reason_code": reason_code or f"ORPHAN_FK_{column.upper()}",
        "condition": F.col(column).isNotNull() & (~F.col(column).isin(keys_list)),
    }


def date_order_check(start_col: str, end_col: str, reason_code: str | None = None) -> dict:
    return {
        "reason_code": reason_code or f"INVALID_DATE_RANGE_{start_col.upper()}_{end_col.upper()}",
        "condition": F.col(start_col).isNotNull() & F.col(end_col).isNotNull() & (F.col(start_col) > F.col(end_col)),
    }


# --------------------------------------------------------------------------
# Dedup + validation engine
# --------------------------------------------------------------------------

def dedup_keep_deterministic(df: DataFrame, keys: list[str], tiebreak_desc_cols: list[str]) -> tuple[DataFrame, int]:
    """Deduplicates a batch by business key, keeping exactly one row per key:
    the row with the greatest tiebreak_desc_cols tuple (per plan.md's
    deterministic dedup rule -- typically (source_file, ingested_at)).
    Returns (deduped_df, rows_dropped_count)."""
    w = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(*[F.col(c).desc() for c in tiebreak_desc_cols])
    ranked = df.withColumn("_rn", F.row_number().over(w))
    before = df.count()
    deduped = ranked.filter(F.col("_rn") == 1).drop("_rn")
    after = deduped.count()
    return deduped, before - after


def validate(df: DataFrame, rules: list[dict]) -> tuple[DataFrame, DataFrame]:
    """Runs every rule against df. Returns (valid_df, quarantine_df).
    quarantine_df has an added `reject_reasons` column: a comma-joined list
    of every reason code that fired for that row."""
    if not rules:
        empty_quarantine = df.limit(0).withColumn(REJECT_REASONS_COL, F.lit(None).cast("string"))
        return df, empty_quarantine

    reason_cols = []
    tagged = df
    for rule in rules:
        flag_col = f"_flag_{rule['reason_code']}"
        tagged = tagged.withColumn(flag_col, F.when(rule["condition"], F.lit(rule["reason_code"])))
        reason_cols.append(flag_col)

    tagged = tagged.withColumn(
        REJECT_REASONS_COL,
        F.array_join(F.array_except(F.array(*[F.col(c) for c in reason_cols]), F.array(F.lit(None).cast("string"))), ","),
    )
    tagged = tagged.drop(*reason_cols)

    quarantine_df = tagged.filter(F.col(REJECT_REASONS_COL) != "")
    valid_df = tagged.filter(F.col(REJECT_REASONS_COL) == "").drop(REJECT_REASONS_COL)
    return valid_df, quarantine_df


# --------------------------------------------------------------------------
# Standalone post-hoc metrics (used by silver_job.py logging and by
# tests/data_quality_checks.py against already-persisted Iceberg tables)
# --------------------------------------------------------------------------

def null_rate(df: DataFrame, column: str) -> float:
    total = df.count()
    if total == 0:
        return 0.0
    nulls = df.filter(F.col(column).isNull()).count()
    return round(nulls / total, 4)


def duplicate_count(df: DataFrame, keys: list[str]) -> int:
    grouped = df.groupBy(*keys).count().filter(F.col("count") > 1)
    return grouped.count()


def orphan_count(df: DataFrame, fk_column: str, ref_df: DataFrame, ref_column: str) -> int:
    ref_keys = {row[0] for row in ref_df.select(ref_column).distinct().collect()}
    return df.filter(F.col(fk_column).isNotNull() & (~F.col(fk_column).isin(list(ref_keys)))).count()


def row_count(df: DataFrame) -> int:
    return df.count()
