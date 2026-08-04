"""Unit tests for every mapping rule in the assignment.

Each test feeds a small in-memory DataFrame through one rule and asserts on
the collected result. Dirty values used here ('mAle', '150-59', 'XXX'...) were
all observed in the real sample file.
"""

import datetime
from decimal import Decimal

import pyspark.sql.functions as F

from transaction_etl import transforms


def _apply(spark, rule_col, rows, columns):
    """Run a single rule column expression over rows and return its values."""
    df = spark.createDataFrame(rows, columns)
    return [r[0] for r in df.select(rule_col.alias("out")).collect()]


class TestParseSourceDate:
    def test_valid_and_malformed(self, spark):
        out = _apply(
            spark,
            transforms.parse_source_date(F.col("d")),
            [("25/04/2019",), (" 01/06/2020 ",), ("2019-04-25",), ("31/02/2020",), (None,)],
            ["d"],
        )
        assert out == [
            datetime.date(2019, 4, 25),
            datetime.date(2020, 6, 1),
            None,  # wrong format
            None,  # impossible date
            None,
        ]


class TestCustomerType:
    def test_rules(self, spark):
        rule = transforms.customer_type(F.col("ct"), F.col("age"), F.col("gender"))
        out = _apply(
            spark,
            rule,
            [
                ("B", "40-49", "Female"),   # passthrough
                ("C", ">=70", "Male"),      # age rule
                ("D", "50-59", "Unknown"),  # gender rule
                ("E", "50-59", "unknown"),  # gender rule, case-insensitive
                ("A", ">=70", "Unknown"),   # both rules
            ],
            ["ct", "age", "gender"],
        )
        assert out == ["B", "Unspecified", "Unspecified", "Unspecified", "Unspecified"]


class TestPostingDate:
    def test_latest_wins_and_null_fallback(self, spark):
        rule = transforms.posting_date(
            transforms.parse_source_date(F.col("post")),
            transforms.parse_source_date(F.col("txn")),
        )
        out = _apply(
            spark,
            rule,
            [
                ("08/05/2019", "05/05/2019"),  # posting later -> posting
                ("05/05/2019", "08/05/2019"),  # transaction later -> transaction
                (None, "05/05/2019"),          # posting missing -> transaction
                ("bad-date", "05/05/2019"),    # posting malformed -> transaction
            ],
            ["post", "txn"],
        )
        assert out == [
            datetime.date(2019, 5, 8),
            datetime.date(2019, 5, 8),
            datetime.date(2019, 5, 5),
            datetime.date(2019, 5, 5),
        ]


class TestDescriptionText:
    def test_cleanup(self, spark):
        rule = transforms.description_text(F.col("desc"), F.col("country"))
        out = _apply(
            spark,
            rule,
            [
                ("LEGAL & GENERAL GROUP ##### GBR", "GBR"),  # both removals
                ("DIAGEO", "GBR"),                            # nothing to remove
                ("ACME CORP USA", "GBR"),                     # trailing token != country: keep
                ("ACME CORP GBR", "GBR"),                     # trailing country, no #####
                ("ACME ##### CORP", "GBR"),                   # ##### mid-string
                ("SOMETHING FRA", None),                      # null country: keep
            ],
            ["desc", "country"],
        )
        assert out == [
            "LEGAL & GENERAL GROUP",
            "DIAGEO",
            "ACME CORP USA",
            "ACME CORP",
            "ACME CORP",
            "SOMETHING FRA",
        ]


class TestEuFlag:
    def test_membership_and_brexit(self, spark):
        rule = transforms.eu_flag(
            F.col("country"), transforms.parse_source_date(F.col("txn"))
        )
        out = _apply(
            spark,
            rule,
            [
                ("LUX", "05/05/2019"),  # EU member
                ("USA", "05/05/2019"),  # not EU
                ("GBR", "31/01/2020"),  # GBR on cutoff day -> still EU
                ("GBR", "01/02/2020"),  # GBR after cutoff -> not EU
                ("GBR", "05/05/2019"),  # GBR before cutoff -> EU
                (None, "05/05/2019"),   # no country -> False
            ],
            ["country", "txn"],
        )
        assert out == [True, False, True, False, True, False]


class TestGenderCode:
    def test_normalization(self, spark):
        out = _apply(
            spark,
            transforms.gender_code(F.col("g")),
            [
                ("Male",), ("Female",), ("Unknown",),
                ("mAle",), ("m",), ("f",), ("F",),  # dirty variants from the file
                ("",), (None,),
            ],
            ["g"],
        )
        assert out == ["M", "F", "X", "M", "M", "F", "F", "X", "X"]


class TestPostCode:
    def test_uk_masking_and_passthrough(self, spark):
        out = _apply(
            spark,
            transforms.post_code(F.col("pc")),
            [
                ("SE23 0AA",),   # PDF example
                ("NW5 0AA",),    # with space
                ("EC10AA",),     # already compact
                ("SW1A 1AA",),   # long outward code
                ("XX12 9XY",),   # not a real postcode but valid UK format
                ("XXX",),        # junk: passthrough
                ("75008",),      # non-UK: passthrough
                (None,),
            ],
            ["pc"],
        )
        assert out == [
            "SE230**",
            "NW50**",
            "EC10**",
            "SW1A1**",
            "XX129**",
            "XXX",
            "75008",
            None,
        ]


class TestNoteText:
    def test_hidden_characters_removed(self, spark):
        out = _apply(
            spark,
            transforms.note_text(F.col("n")),
            [
                ("a\tb",),                 # tab (present in the sample file)
                ("line1\nline2",),         # newline
                ("line1\r\nline2",),       # carriage return + newline
                ("España",),          # accented text preserved
                ("日本で",),   # non-Latin text preserved
                ("zero​width",),      # zero-width space removed
                (None,),
            ],
            ["n"],
        )
        assert out == ["ab", "line1line2", "line1line2", "España", "日本で", "zerowidth", None]


class TestEndToEnd:
    def test_full_mapping_row(self, spark):
        bronze = spark.createDataFrame(
            [
                (
                    "2", "E", "25/04/2019", "28/04/2019", "1.45",
                    "LEGAL & GENERAL GROUP ##### GBR", "GBR", "40-49", "Male",
                    "E0799D1C", "CUST001517826959", "NW5 0AA", "note\nwith newline",
                )
            ],
            [
                "record_id", "customer_type", "transaction_date", "posting_date",
                "transaction_amount", "Txn_Description", "country", "Age_Band",
                "gender", "Transaction_id", "customer_key", "customer_postcode",
                "Notes",
            ],
        )
        row = transforms.transform_bronze_to_silver(bronze, "2026-08-04").collect()[0]

        assert row.FILE_DATE == datetime.date(2026, 8, 4)
        assert row.RECORD_ID == 2
        assert row.CUSTOMER_TYPE == "E"
        assert row.TRANSACTION_DATE == datetime.date(2019, 4, 25)
        assert row.POSTING_DATE == datetime.date(2019, 4, 28)
        assert row.TRANSACTION_AMOUNT == Decimal("1.45")
        assert row.DESCRIPTION_TEXT == "LEGAL & GENERAL GROUP"
        assert row.ORIGINAL_DESCRIPTION_TEXT == "LEGAL & GENERAL GROUP ##### GBR"
        assert row.COUNTRY_CODE == "GBR"
        assert row.EU_FLAG is True  # GBR transaction before Brexit cutoff
        assert row.AGE_BAND == "40-49"
        assert row.GENDER_CODE == "M"
        assert row.TRANSACTION_ID == "E0799D1C"
        assert row.CUSTOMER_KEY == "CUST001517826959"
        assert row.POST_CODE == "NW50**"
        assert row.NOTE_TEXT == "notewith newline"
