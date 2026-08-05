"""Shared Iceberg helper functions for the retail Medallion Glue jobs.

Plain PySpark functions with no AWS SDK calls, so their logic can be reasoned
about (and unit tested with a local Iceberg/Hadoop catalog) independently of
a live Glue environment. They assume the calling job has already configured
a SparkSession with the Iceberg + Glue Catalog extensions -- see
infrastructure/cloudformation/04-glue-jobs.yaml DefaultArguments:
  --datalake-formats            iceberg
  --conf                        spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
                                 spark.sql.catalog.glue_catalog.warehouse=s3://.../warehouse
                                 spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
                                 spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

Deployment note: zip this jobs/common/ directory (keeping the `common/`
folder as the zip root) as common.zip, upload to the scripts bucket, and set
each Glue job's --extra-py-files to its S3 path. Job scripts then import via
`from common import iceberg_utils`.
"""
from __future__ import annotations

CATALOG_NAME = "glue_catalog"

# Sensible, table-agnostic Iceberg defaults. format-version=2 is required for
# row-level MERGE/DELETE (used by Silver upserts and Bronze replay deletes).
DEFAULT_TBLPROPERTIES = {
    "format-version": "2",
    "write.target-file-size-bytes": "134217728",  # 128MB
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "20",
}


def fqtn(database: str, table: str) -> str:
    """Fully-qualified Iceberg table name for the configured Glue catalog."""
    return f"{CATALOG_NAME}.{database}.{table}"


def table_exists(spark, database: str, table: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {fqtn(database, table)}")
        return True
    except Exception:
        return False


def create_table_if_not_exists(spark, database: str, table: str, schema_ddl: str,
                                location: str, partition_clause: str = "",
                                extra_tblproperties: dict | None = None) -> None:
    """Creates an Iceberg table via Spark SQL DDL if it doesn't already exist."""
    tblprops = dict(DEFAULT_TBLPROPERTIES)
    if extra_tblproperties:
        tblprops.update(extra_tblproperties)
    props_sql = ", ".join(f"'{k}'='{v}'" for k, v in tblprops.items())
    partition_sql = f"PARTITIONED BY ({partition_clause})" if partition_clause else ""
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {fqtn(database, table)} ({schema_ddl}) "
        f"USING iceberg {partition_sql} LOCATION '{location}' "
        f"TBLPROPERTIES ({props_sql})"
    )
    spark.sql(ddl)


def add_column_if_not_exists(spark, database: str, table: str, column_name: str,
                              column_type: str) -> None:
    """Safe, additive schema evolution: adds a nullable column if it doesn't
    already exist. Never renames/drops/retypes -- those are handled as
    explicit, reviewed DDL, not an automatic helper, since they can break
    downstream readers."""
    existing = {f.name for f in spark.table(fqtn(database, table)).schema.fields}
    if column_name not in existing:
        spark.sql(f"ALTER TABLE {fqtn(database, table)} ADD COLUMN {column_name} {column_type}")


def append_dataframe(df, database: str, table: str) -> None:
    """Atomic Iceberg append -- the whole batch commits as one snapshot, or
    none of it does, satisfying the atomic-write / failure-safe requirement."""
    df.writeTo(fqtn(database, table)).append()


def overwrite_partitions(df, database: str, table: str) -> None:
    """Atomically replaces only the partitions present in df (dynamic
    partition overwrite). Used by Gold to recompute affected date partitions
    without touching unrelated history -- also idempotent on rerun, since
    recomputing the same partition with the same inputs yields the same
    output rows."""
    df.writeTo(fqtn(database, table)).overwritePartitions()


def batch_already_loaded(spark, database: str, table: str, batch_id: str) -> bool:
    """Idempotency guard for Bronze's append-only history: detects whether
    this batch_id has already been committed, so a plain rerun (retry after
    a transient failure, or an accidental duplicate trigger) doesn't create
    duplicate raw history."""
    if not table_exists(spark, database, table):
        return False
    result = spark.sql(
        f"SELECT 1 AS x FROM {fqtn(database, table)} WHERE batch_id = '{batch_id}' LIMIT 1"
    )
    return result.limit(1).count() > 0


def delete_batch(spark, database: str, table: str, batch_id: str) -> None:
    """Removes a previously-loaded batch's rows. Used for explicit
    --force_reload replay of a batch (backfill correction), not for normal
    reruns -- normal reruns are short-circuited by batch_already_loaded."""
    if table_exists(spark, database, table):
        spark.sql(f"DELETE FROM {fqtn(database, table)} WHERE batch_id = '{batch_id}'")


def merge_into(spark, source_view: str, database: str, table: str, merge_keys: list[str],
               update_columns: list[str], insert_columns: list[str]) -> None:
    """Simple upsert MERGE with no CDC/op_type columns required:
    business_key exists in target -> UPDATE, else -> INSERT.
    See plan.md 'Simplified Incremental Logic'. `source_view` must already be
    deduplicated by merge_keys (one deterministic row per key) -- Iceberg's
    MERGE INTO raises an error if the source produces multiple matches for
    the same target row."""
    on_clause = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
    set_clause = ", ".join(f"t.{c} = s.{c}" for c in update_columns)
    insert_cols_sql = ", ".join(insert_columns)
    insert_vals_sql = ", ".join(f"s.{c}" for c in insert_columns)
    sql = f"""
        MERGE INTO {fqtn(database, table)} t
        USING {source_view} s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql})
    """
    spark.sql(sql)
