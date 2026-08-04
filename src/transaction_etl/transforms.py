"""Bronze -> silver transformation logic.

Each mapping rule from the assignment is a small function returning a Spark
Column expression, so every rule can be unit-tested in isolation. All logic
uses native Spark column functions (no Python UDFs) so it stays fast at scale.

Dates are parsed with try_to_date / try_cast: malformed values become NULL
instead of failing the job (Spark 4 runs in ANSI mode by default), and the
data-quality layer decides what to do with the NULLs.
"""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

from transaction_etl.schema import (
    EU_COUNTRY_CODES,
    GBR_EU_CUTOFF,
    SOURCE_DATE_FORMAT,
    UK_POSTCODE_REGEX,
)


def parse_source_date(col: Column) -> Column:
    """Parse a dd/MM/yyyy string to a date; malformed values become NULL.

    PySpark has no try_to_date, so parse via try_to_timestamp (NULL on
    malformed input) and cast down to date, which cannot fail.
    """
    return F.try_to_timestamp(F.trim(col), F.lit(SOURCE_DATE_FORMAT)).cast("date")


def customer_type(customer_type_col: Column, age_band: Column, gender: Column) -> Column:
    """'Unspecified' when age >= 70 or gender is unknown, else the source value.

    Age arrives only as a band, so "age >= 70" is detected via the '>=70' band.
    """
    is_70_plus = F.trim(age_band) == ">=70"
    is_unknown_gender = F.lower(F.trim(gender)) == "unknown"
    return F.when(is_70_plus | is_unknown_gender, F.lit("Unspecified")).otherwise(
        F.trim(customer_type_col)
    )


def posting_date(posting: Column, transaction: Column) -> Column:
    """Latest of posting_date and transaction_date (both already parsed).

    greatest() ignores NULLs, so if one date failed to parse the other wins.
    """
    return F.greatest(posting, transaction)


def description_text(description: Column, country: Column) -> Column:
    """Clean Txn_Description: drop '#####' and a trailing country code.

    The trailing token is removed only when it equals the row's own country,
    per the mapping (e.g. 'ACME ##### GBR' with country=GBR -> 'ACME').
    """
    base = F.trim(description)
    ctry = F.trim(country)
    # Strip the trailing country code only when it matches this row's country.
    ends_with_country = country.isNotNull() & base.endswith(
        F.concat(F.lit(" "), ctry)
    )
    without_country = base.substr(F.lit(1), F.length(base) - F.length(ctry) - 1)
    cleaned = F.when(ends_with_country, without_country).otherwise(base)
    # Drop the '#####' placeholder wherever it appears, then tidy whitespace.
    cleaned = F.regexp_replace(cleaned, r"#####", "")
    cleaned = F.regexp_replace(cleaned, r"\s{2,}", " ")
    return F.trim(cleaned)


def eu_flag(country: Column, transaction_date: Column) -> Column:
    """True for EU members; True for GBR on/before the Brexit cutoff; else False."""
    is_eu_member = F.trim(country).isin(EU_COUNTRY_CODES)
    is_pre_brexit_gbr = (F.trim(country) == "GBR") & (
        transaction_date <= F.lit(GBR_EU_CUTOFF).cast("date")
    )
    return F.when(is_eu_member | is_pre_brexit_gbr, F.lit(True)).otherwise(F.lit(False))


def gender_code(gender: Column) -> Column:
    """M if male, F if female, X otherwise (case-insensitive, tolerant of 'm'/'f')."""
    normalized = F.lower(F.trim(gender))
    return (
        F.when(normalized.isin("male", "m"), F.lit("M"))
        .when(normalized.isin("female", "f"), F.lit("F"))
        .otherwise(F.lit("X"))
    )


def post_code(postcode: Column) -> Column:
    """Mask UK-format postcodes; pass everything else through unchanged.

    UK format check is done on the space-stripped, uppercased value. When it
    matches, spaces are removed and the last two characters are replaced
    with '**' (SE23 0AA -> SE230**).
    """
    compact = F.upper(F.regexp_replace(F.trim(postcode), r"\s+", ""))
    is_uk_format = compact.rlike(UK_POSTCODE_REGEX)
    masked = F.concat(compact.substr(F.lit(1), F.length(compact) - 2), F.lit("**"))
    return F.when(is_uk_format, masked).otherwise(postcode)


def note_text(notes: Column) -> Column:
    """Remove newline and hidden characters from free-text notes.

    \\p{C} covers all Unicode 'Other' categories: control characters (newlines,
    tabs, NUL...), invisible format characters (zero-width space, BOM...) and
    unassigned code points. Printable text - including accents and non-Latin
    scripts - is preserved.
    """
    return F.regexp_replace(notes, r"\p{C}+", "")


def transform_bronze_to_silver(bronze: DataFrame, file_date: str) -> DataFrame:
    """Apply the full assignment mapping to a bronze DataFrame.

    Args:
        bronze: raw string-typed DataFrame with the source column names.
        file_date: the date the file was received, 'yyyy-MM-dd'.
    """
    txn_date = parse_source_date(F.col("transaction_date"))
    post_date = parse_source_date(F.col("posting_date"))

    return bronze.select(
        F.lit(file_date).cast("date").alias("FILE_DATE"),
        F.col("record_id").cast("bigint").alias("RECORD_ID"),
        customer_type(
            F.col("customer_type"), F.col("Age_Band"), F.col("gender")
        ).alias("CUSTOMER_TYPE"),
        txn_date.alias("TRANSACTION_DATE"),
        posting_date(post_date, txn_date).alias("POSTING_DATE"),
        F.trim("transaction_amount").try_cast("decimal(18,2)").alias("TRANSACTION_AMOUNT"),
        description_text(F.col("Txn_Description"), F.col("country")).alias(
            "DESCRIPTION_TEXT"
        ),
        F.col("Txn_Description").alias("ORIGINAL_DESCRIPTION_TEXT"),
        F.trim("country").alias("COUNTRY_CODE"),
        eu_flag(F.col("country"), txn_date).alias("EU_FLAG"),
        F.trim("Age_Band").alias("AGE_BAND"),
        gender_code(F.col("gender")).alias("GENDER_CODE"),
        F.trim("Transaction_id").alias("TRANSACTION_ID"),
        F.trim("customer_key").alias("CUSTOMER_KEY"),
        post_code(F.col("customer_postcode")).alias("POST_CODE"),
        note_text(F.col("Notes")).alias("NOTE_TEXT"),
    )
