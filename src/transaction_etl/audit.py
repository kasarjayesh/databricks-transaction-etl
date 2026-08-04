"""Run-audit logging: one row per pipeline stage per run.

The audit table is the pipeline's monitoring backbone: row counts in/out and
quarantined per stage, per file date, per run. The dashboard's data-quality
tab reads it, and sudden volume changes are visible immediately.
"""

from __future__ import annotations

import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

AUDIT_SCHEMA = StructType(
    [
        StructField("run_ts", TimestampType()),
        StructField("stage", StringType()),
        StructField("file_date", StringType()),
        StructField("rows_in", LongType()),
        StructField("rows_out", LongType()),
        StructField("rows_quarantined", LongType()),
        StructField("notes", StringType()),
    ]
)


def log_run(
    spark: SparkSession,
    table_fqn: str,
    stage: str,
    file_date: str,
    rows_in: int,
    rows_out: int,
    rows_quarantined: int = 0,
    notes: str = "",
) -> None:
    """Append one audit row for a completed stage."""
    row = [
        (
            datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None),
            stage,
            file_date,
            rows_in,
            rows_out,
            rows_quarantined,
            notes,
        )
    ]
    spark.createDataFrame(row, AUDIT_SCHEMA).write.mode("append").saveAsTable(
        table_fqn
    )
