"""Structured logging + run-metrics helpers for the retail Medallion Glue jobs.

Emits one JSON log line per job run to stdout (captured by Glue into
CloudWatch Logs), so row counts / rejected counts / duration / watermark can
be queried with CloudWatch Logs Insights, e.g.:

  fields job_name, table, status, duration_seconds, rows_written, rows_rejected
  | filter event = "glue_job_run_metrics"
  | sort run_ts desc

Also (best-effort) persists the same record as a row in the
gold.pipeline_run_log Iceberg table for self-service SQL/Athena auditing.
"""
from __future__ import annotations

import json
import logging
import sys
import time
import uuid
from datetime import datetime, timezone

from common.iceberg_utils import append_dataframe, create_table_if_not_exists

RUN_LOG_TABLE = "pipeline_run_log"
RUN_LOG_SCHEMA_DDL = (
    "job_name STRING, layer STRING, table_name STRING, batch_id STRING, "
    "load_id STRING, status STRING, duration_seconds DOUBLE, run_ts TIMESTAMP, "
    "rows_read BIGINT, rows_written BIGINT, rows_rejected BIGINT, "
    "watermark STRING, error STRING"
)


def get_logger(job_name: str) -> logging.Logger:
    logger = logging.getLogger(job_name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.propagate = False
    return logger


def new_batch_id() -> str:
    return uuid.uuid4().hex


def default_load_id(table: str, load_mode: str) -> str:
    return f"{table}_{load_mode}_{datetime.now(timezone.utc):%Y%m%d%H%M%S}"


class RunMetrics:
    """Accumulates observability metrics for a single table's processing
    within a job run and emits them as one structured JSON log line."""

    def __init__(self, job_name: str, layer: str, table: str, batch_id: str, load_id: str):
        self.job_name = job_name
        self.layer = layer
        self.table = table
        self.batch_id = batch_id
        self.load_id = load_id
        self.started_at = time.time()
        self.metrics: dict = {}

    def set(self, key: str, value) -> None:
        self.metrics[key] = value

    def increment(self, key: str, amount: int = 1) -> None:
        self.metrics[key] = self.metrics.get(key, 0) + amount

    def finalize(self, status: str = "SUCCESS", error: str | None = None) -> dict:
        duration_seconds = round(time.time() - self.started_at, 2)
        record = {
            "event": "glue_job_run_metrics",
            "job_name": self.job_name,
            "layer": self.layer,
            "table": self.table,
            "batch_id": self.batch_id,
            "load_id": self.load_id,
            "status": status,
            "duration_seconds": duration_seconds,
            "run_ts": datetime.now(timezone.utc).isoformat(),
        }
        record.update(self.metrics)
        if error:
            record["error"] = error
        return record

    def log(self, logger: logging.Logger, status: str = "SUCCESS", error: str | None = None) -> dict:
        record = self.finalize(status=status, error=error)
        logger.info(json.dumps(record, default=str))
        return record


def persist_run_log(spark, gold_database: str, warehouse_gold_location: str, record: dict) -> None:
    """Appends a run-metrics record to gold.pipeline_run_log. Best-effort:
    failures here must never fail the parent ETL job -- observability
    plumbing should not be a new source of pipeline outages."""
    try:
        location = f"{warehouse_gold_location.rstrip('/')}/{RUN_LOG_TABLE}/"
        create_table_if_not_exists(
            spark, gold_database, RUN_LOG_TABLE, RUN_LOG_SCHEMA_DDL, location,
            partition_clause="days(run_ts)",
        )
        row = {
            "job_name": record.get("job_name"),
            "layer": record.get("layer"),
            "table_name": record.get("table"),
            "batch_id": record.get("batch_id"),
            "load_id": record.get("load_id"),
            "status": record.get("status"),
            "duration_seconds": float(record.get("duration_seconds", 0.0)),
            "run_ts": record.get("run_ts"),
            "rows_read": int(record.get("rows_read", 0) or 0),
            "rows_written": int(record.get("rows_written", 0) or 0),
            "rows_rejected": int(record.get("rows_rejected", 0) or 0),
            "watermark": record.get("watermark"),
            "error": record.get("error"),
        }
        df = spark.createDataFrame([row])
        append_dataframe(df, gold_database, RUN_LOG_TABLE)
    except Exception:
        pass
