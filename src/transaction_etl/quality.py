"""Data-quality checks applied between the silver transform and the write.

Two severity levels:
- CRITICAL: the row is unusable for reporting; it is diverted to a quarantine
  table (with the reason) instead of silver. Nothing is silently dropped.
- WARNING: the row is kept in silver but tagged, so data issues stay visible
  and can be monitored on the dashboard without blocking the pipeline.
"""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

from transaction_etl.schema import VALID_AGE_BANDS

QUARANTINE_REASON_COL = "DQ_QUARANTINE_REASONS"
WARNING_COL = "DQ_WARNINGS"


def critical_rules() -> dict:
    """Rules that make a row unusable. Name -> violation condition."""
    return {
        "missing_record_id": F.col("RECORD_ID").isNull(),
        "missing_transaction_id": F.col("TRANSACTION_ID").isNull()
        | (F.col("TRANSACTION_ID") == ""),
        "missing_transaction_date": F.col("TRANSACTION_DATE").isNull(),
        "missing_transaction_amount": F.col("TRANSACTION_AMOUNT").isNull(),
    }


def warning_rules() -> dict:
    """Rules worth monitoring but not worth rejecting the row for."""
    return {
        "missing_country": F.col("COUNTRY_CODE").isNull()
        | (F.col("COUNTRY_CODE") == ""),
        "invalid_age_band": ~F.col("AGE_BAND").isin(VALID_AGE_BANDS),
        "duplicate_transaction_id": F.count("*").over(
            Window.partitionBy("TRANSACTION_ID")
        )
        > 1,
        "future_transaction_date": F.col("TRANSACTION_DATE") > F.col("FILE_DATE"),
    }


def _violation_array(rules: dict):
    """Array column of the names of all violated rules (empty when clean)."""
    flags = [F.when(condition, F.lit(name)) for name, condition in rules.items()]
    return F.array_compact(F.array(*flags))


def apply_quality_checks(silver: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split a transformed DataFrame into (clean, quarantined).

    The clean DataFrame keeps a DQ_WARNINGS column; the quarantined one keeps
    a DQ_QUARANTINE_REASONS column so every rejection is explainable.
    """
    checked = silver.withColumn(
        QUARANTINE_REASON_COL, _violation_array(critical_rules())
    ).withColumn(WARNING_COL, _violation_array(warning_rules()))

    clean = checked.filter(F.size(QUARANTINE_REASON_COL) == 0).drop(
        QUARANTINE_REASON_COL
    )
    quarantined = checked.filter(F.size(QUARANTINE_REASON_COL) > 0).drop(WARNING_COL)
    return clean, quarantined
