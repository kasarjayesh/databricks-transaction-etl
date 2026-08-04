"""Unit tests for the data-quality split (clean vs quarantined rows)."""

import datetime
from decimal import Decimal

from transaction_etl.quality import (
    QUARANTINE_REASON_COL,
    WARNING_COL,
    apply_quality_checks,
)

COLUMNS = [
    "FILE_DATE",
    "RECORD_ID",
    "TRANSACTION_ID",
    "TRANSACTION_DATE",
    "TRANSACTION_AMOUNT",
    "COUNTRY_CODE",
    "AGE_BAND",
]

FILE_DATE = datetime.date(2026, 8, 4)


def _row(
    record_id=1,
    transaction_id="T1",
    transaction_date=datetime.date(2019, 5, 5),
    amount=Decimal("9.55"),
    country="GBR",
    age_band="40-49",
):
    return (FILE_DATE, record_id, transaction_id, transaction_date, amount, country, age_band)


def test_clean_row_passes_without_warnings(spark):
    df = spark.createDataFrame([_row()], COLUMNS)
    clean, quarantined = apply_quality_checks(df)
    assert clean.count() == 1
    assert quarantined.count() == 0
    assert clean.collect()[0][WARNING_COL] == []


def test_critical_violations_are_quarantined_with_reasons(spark):
    df = spark.createDataFrame(
        [
            _row(record_id=None),                      # critical
            _row(record_id=2, transaction_date=None),  # critical
            _row(record_id=3),                         # clean
        ],
        COLUMNS,
    )
    clean, quarantined = apply_quality_checks(df)
    assert clean.count() == 1
    reasons = {
        r["RECORD_ID"]: r[QUARANTINE_REASON_COL] for r in quarantined.collect()
    }
    assert reasons[None] == ["missing_record_id"]
    assert reasons[2] == ["missing_transaction_date"]


def test_warnings_keep_rows_but_tag_them(spark):
    df = spark.createDataFrame(
        [
            _row(record_id=1, country=None, age_band="150-59"),
            _row(record_id=2, transaction_id="DUP"),
            _row(record_id=3, transaction_id="DUP"),
        ],
        COLUMNS,
    )
    clean, quarantined = apply_quality_checks(df)
    assert quarantined.count() == 0
    warnings = {r["RECORD_ID"]: set(r[WARNING_COL]) for r in clean.collect()}
    assert warnings[1] == {"missing_country", "invalid_age_band"}
    assert warnings[2] == {"duplicate_transaction_id"}
    assert warnings[3] == {"duplicate_transaction_id"}
