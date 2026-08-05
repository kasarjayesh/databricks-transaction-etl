# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: reporting aggregates for the dashboard
# MAGIC
# MAGIC Rebuilds small, query-friendly aggregate tables from the whole silver
# MAGIC table. At the current scale a full rebuild is trivially cheap and always
# MAGIC consistent; incremental refresh is a documented future improvement.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

import datetime

from transaction_etl.audit import log_run

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "transactions")
dbutils.widgets.text("file_date", "", "File date (yyyy-mm-dd, empty = today)")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
file_date = dbutils.widgets.get("file_date") or datetime.date.today().isoformat()

fqn = f"`{catalog}`.`{schema}`"

# COMMAND ----------

# MAGIC %md ## Daily transaction summary

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {fqn}.gold_daily_summary
    COMMENT 'Transaction volume and value per transaction date' AS
    SELECT
        TRANSACTION_DATE,
        COUNT(*)                            AS txn_count,
        SUM(TRANSACTION_AMOUNT)             AS total_amount,
        ROUND(AVG(TRANSACTION_AMOUNT), 2)   AS avg_amount,
        SUM(CASE WHEN EU_FLAG THEN TRANSACTION_AMOUNT ELSE 0 END) AS eu_amount
    FROM {fqn}.silver_transactions
    GROUP BY TRANSACTION_DATE
    """
)

# COMMAND ----------

# MAGIC %md ## Country and EU split

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {fqn}.gold_country_summary
    COMMENT 'Volume and value per country with EU flag' AS
    SELECT
        COUNTRY_CODE,
        EU_FLAG,
        COUNT(*)                          AS txn_count,
        SUM(TRANSACTION_AMOUNT)           AS total_amount,
        ROUND(AVG(TRANSACTION_AMOUNT), 2) AS avg_amount
    FROM {fqn}.silver_transactions
    GROUP BY COUNTRY_CODE, EU_FLAG
    """
)

# COMMAND ----------

# MAGIC %md ## Customer segments

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {fqn}.gold_customer_segments
    COMMENT 'Volume and value by customer type, age band and gender' AS
    SELECT
        CUSTOMER_TYPE,
        AGE_BAND,
        GENDER_CODE,
        COUNT(*)                  AS txn_count,
        SUM(TRANSACTION_AMOUNT)   AS total_amount,
        COUNT(DISTINCT CUSTOMER_KEY) AS customer_count
    FROM {fqn}.silver_transactions
    GROUP BY CUSTOMER_TYPE, AGE_BAND, GENDER_CODE
    """
)

# COMMAND ----------

# MAGIC %md ## Top merchants (by cleaned description)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {fqn}.gold_merchant_summary
    COMMENT 'Volume and value per cleaned transaction description' AS
    SELECT
        DESCRIPTION_TEXT,
        COUNT(*)                AS txn_count,
        SUM(TRANSACTION_AMOUNT) AS total_amount
    FROM {fqn}.silver_transactions
    GROUP BY DESCRIPTION_TEXT
    """
)

# COMMAND ----------

# MAGIC %md ## Data-quality metrics

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {fqn}.gold_dq_summary
    COMMENT 'Clean vs quarantined rows and warning counts per file date' AS
    WITH warnings AS (
        SELECT FILE_DATE, warning, COUNT(*) AS cnt
        FROM {fqn}.silver_transactions
        LATERAL VIEW EXPLODE(DQ_WARNINGS) AS warning
        GROUP BY FILE_DATE, warning
    ),
    quarantine AS (
        SELECT FILE_DATE, reason AS warning, COUNT(*) AS cnt
        FROM {fqn}.quarantine_transactions
        LATERAL VIEW EXPLODE(DQ_QUARANTINE_REASONS) AS reason
        GROUP BY FILE_DATE, reason
    )
    SELECT FILE_DATE, warning AS issue, cnt, 'warning' AS severity FROM warnings
    UNION ALL
    SELECT FILE_DATE, warning AS issue, cnt, 'quarantined' AS severity FROM quarantine
    """
)

# COMMAND ----------

gold_tables = [
    "gold_daily_summary",
    "gold_country_summary",
    "gold_customer_segments",
    "gold_merchant_summary",
    "gold_dq_summary",
]
total = sum(spark.table(f"{fqn}.{t}").count() for t in gold_tables)
# Gold rebuilds from the WHOLE silver table (not just this file_date), so
# rows_in is the full silver row count feeding the aggregation.
silver_rows = spark.table(f"{fqn}.silver_transactions").count()
log_run(
    spark,
    f"{catalog}.{schema}.etl_run_log",
    "gold",
    file_date,
    rows_in=silver_rows,
    rows_out=total,
    notes=f"rebuilt {len(gold_tables)} gold tables from full silver",
)
print(f"Gold complete: {len(gold_tables)} tables, {total} aggregate rows")
