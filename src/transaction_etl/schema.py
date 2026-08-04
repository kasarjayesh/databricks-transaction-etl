"""Schemas and reference data for the transaction pipeline.

The bronze schema declares every source column as a string: ingestion must
never fail because of a bad value in one field. Typing/validation happens in
the silver layer where bad rows can be quarantined individually.
"""

from pyspark.sql.types import StringType, StructField, StructType

# Source file columns, in file order (header names as they appear in the CSV).
BRONZE_SCHEMA = StructType(
    [
        StructField("record_id", StringType()),
        StructField("customer_type", StringType()),
        StructField("transaction_date", StringType()),
        StructField("posting_date", StringType()),
        StructField("transaction_amount", StringType()),
        StructField("Txn_Description", StringType()),
        StructField("country", StringType()),
        StructField("Age_Band", StringType()),
        StructField("gender", StringType()),
        StructField("Transaction_id", StringType()),
        StructField("customer_key", StringType()),
        StructField("customer_postcode", StringType()),
        StructField("Notes", StringType()),
    ]
)

# Source files use day-first dates, e.g. 25/04/2019.
SOURCE_DATE_FORMAT = "dd/MM/yyyy"

# EU member country codes as listed in the assignment mapping.
EU_COUNTRY_CODES = [
    "AUT", "BEL", "BGR", "HRV", "CYP", "CZE", "DNK", "EST", "FIN",
    "FRA", "DEU", "GRC", "HUN", "IRL", "ITA", "LVA", "LTU", "LUX",
    "MLT", "NLD", "POL", "PRT", "ROU", "SVK", "SVN", "ESP", "SWE",
]

# Per the assignment: GBR transactions count as EU up to and including
# this date (Brexit withdrawal date, 31 January 2020).
GBR_EU_CUTOFF = "2020-01-31"

# UK postcode format, validated after removing spaces and uppercasing:
# outward code (area letters + district) followed by inward code (digit +
# two letters). Format check only — no lookup against real postcodes.
UK_POSTCODE_REGEX = r"^[A-Z]{1,2}[0-9][A-Z0-9]?[0-9][A-Z]{2}$"

# Expected age band values; anything else is flagged by data-quality checks.
VALID_AGE_BANDS = ["<30", "30-39", "40-49", "50-59", "60-69", ">=70"]
