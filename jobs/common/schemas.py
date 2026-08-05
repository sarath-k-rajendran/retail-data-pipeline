"""Single source of truth for source-file schemas and partition specs,
shared by bronze_job.py and silver_job.py so the two layers never drift.

Partitioning rationale is documented in plan.md Phase 0 ("Partitioning
strategy (finalized)"): Bronze/Silver fact tables partition on business
event date (days(<ts column>)); small reference tables are unpartitioned.
"""
from __future__ import annotations

# --------------------------------------------------------------------------
# Bronze: one entry per raw source file. `ddl` matches the CSV column order
# exactly (used for the Spark CSV reader's explicit schema). `partition_clause`
# is the Iceberg PARTITIONED BY expression (empty string = unpartitioned).
# --------------------------------------------------------------------------
BRONZE_SOURCES = {
    "product": {
        "ddl": (
            "product_id STRING, product_name STRING, category STRING, "
            "sub_category STRING, brand STRING, cost DOUBLE, list_price DOUBLE, "
            "active_flag STRING, created_at DATE"
        ),
        "partition_clause": "",
    },
    "store": {
        "ddl": (
            "store_id STRING, store_name STRING, region STRING, state STRING, "
            "city STRING, channel STRING, open_date DATE"
        ),
        "partition_clause": "",
    },
    "promotion": {
        "ddl": (
            "promo_id STRING, promo_name STRING, product_id STRING, "
            "discount_pct INT, start_date DATE, end_date DATE"
        ),
        "partition_clause": "",
    },
    "orders": {
        "ddl": (
            "order_id STRING, order_line_id INT, order_ts TIMESTAMP, store_id STRING, "
            "product_id STRING, promo_id STRING, qty INT, unit_price DOUBLE, "
            "discount_amt DOUBLE, channel STRING, customer_id STRING, customer_email STRING"
        ),
        "partition_clause": "days(order_ts)",
    },
    "returns": {
        "ddl": (
            "return_id STRING, order_id STRING, order_line_id INT, return_ts TIMESTAMP, "
            "qty INT, reason_code STRING"
        ),
        "partition_clause": "days(return_ts)",
    },
    "web_sessions": {
        "ddl": (
            "session_id STRING, customer_id STRING, session_ts TIMESTAMP, store_id STRING, "
            "channel STRING, converted_flag INT, order_id STRING"
        ),
        "partition_clause": "days(session_ts)",
    },
    "inventory_snapshot": {
        "ddl": (
            "store_id STRING, product_id STRING, snapshot_date DATE, "
            "on_hand_qty INT, on_order_qty INT"
        ),
        "partition_clause": "days(snapshot_date)",
    },
}

BRONZE_METADATA_DDL = "ingested_at TIMESTAMP, source_file STRING, batch_id STRING, load_id STRING"
BRONZE_METADATA_COLUMNS = ["ingested_at", "source_file", "batch_id", "load_id"]

# --------------------------------------------------------------------------
# Silver: business/merge keys per conformed table, used by silver_job.py for
# in-batch dedup and the MERGE INTO ... ON clause.
# --------------------------------------------------------------------------
SILVER_BUSINESS_KEYS = {
    "fact_order_lines": ["order_id", "order_line_id"],
    "fact_returns": ["return_id"],
    "dim_product": ["product_id"],
    "dim_store": ["store_id"],
    "fact_web_sessions": ["session_id"],
    "fact_inventory_snapshot": ["store_id", "product_id", "snapshot_date"],
}
