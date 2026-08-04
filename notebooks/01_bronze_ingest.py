# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingest the daily transaction file
# MAGIC
# MAGIC Reads the raw CSV for one `file_date` from the landing volume and lands it
# MAGIC in the bronze Delta table **exactly as received** (all columns as strings)
# MAGIC plus ingestion metadata. Typing and cleansing happen in silver.
# MAGIC
# MAGIC Re-runnable: writing uses `replaceWhere` on `FILE_DATE`, so re-processing a
# MAGIC day replaces that day's data instead of duplicating it.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

import datetime

from pyspark.sql import functions as F

from transaction_etl.audit import log_run
from transaction_etl.schema import BRONZE_SCHEMA

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "transactions")
dbutils.widgets.text("file_date", "", "File date (yyyy-mm-dd, empty = today)")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
file_date = dbutils.widgets.get("file_date") or datetime.date.today().isoformat()
datetime.date.fromisoformat(file_date)  # fail fast on a malformed parameter

landing_dir = f"/Volumes/{catalog}/{schema}/landing/{file_date}"
bronze_table = f"{catalog}.{schema}.bronze_transactions"
audit_table = f"{catalog}.{schema}.etl_run_log"
print(f"Ingesting {landing_dir} -> {bronze_table}")

# COMMAND ----------

raw = (
    spark.read.format("csv")
    .schema(BRONZE_SCHEMA)  # enforce known layout; never infer in production
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("multiLine", True)  # Notes can contain embedded newlines
    .option("escape", '"')
    .load(landing_dir)
)

bronze = raw.select(
    F.lit(file_date).cast("date").alias("FILE_DATE"),
    "*",
    F.col("_metadata.file_path").alias("_source_file"),
    F.current_timestamp().alias("_ingested_at"),
)

# COMMAND ----------

(
    bronze.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"FILE_DATE = '{file_date}'")
    .partitionBy("FILE_DATE")
    .saveAsTable(bronze_table)
)

rows = spark.table(bronze_table).filter(F.col("FILE_DATE") == file_date).count()
log_run(spark, audit_table, "bronze", file_date, rows_in=rows, rows_out=rows)
print(f"Bronze complete: {rows} rows for {file_date}")
