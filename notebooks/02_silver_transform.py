# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: apply the mapping and data-quality rules
# MAGIC
# MAGIC Takes one `file_date` of bronze data and:
# MAGIC 1. applies the full assignment field mapping (`transaction_etl.transforms`),
# MAGIC 2. splits rows into **clean** (-> silver table, with warning tags) and
# MAGIC    **quarantined** (-> quarantine table, with rejection reasons),
# MAGIC 3. records row counts in the audit table.
# MAGIC
# MAGIC All transformation logic lives in the unit-tested `transaction_etl` package;
# MAGIC this notebook only orchestrates.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

import datetime

from pyspark.sql import functions as F

from transaction_etl.audit import log_run
from transaction_etl.quality import apply_quality_checks
from transaction_etl.transforms import transform_bronze_to_silver

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "transactions")
dbutils.widgets.text("file_date", "", "File date (yyyy-mm-dd, empty = today)")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
file_date = dbutils.widgets.get("file_date") or datetime.date.today().isoformat()
datetime.date.fromisoformat(file_date)

bronze_table = f"{catalog}.{schema}.bronze_transactions"
silver_table = f"{catalog}.{schema}.silver_transactions"
quarantine_table = f"{catalog}.{schema}.quarantine_transactions"
audit_table = f"{catalog}.{schema}.etl_run_log"

# COMMAND ----------

bronze = spark.table(bronze_table).filter(F.col("FILE_DATE") == file_date)
rows_in = bronze.count()
if rows_in == 0:
    raise ValueError(
        f"No bronze rows for FILE_DATE={file_date}. Run bronze ingestion first."
    )

silver_all = transform_bronze_to_silver(bronze, file_date)
clean, quarantined = apply_quality_checks(silver_all)

# COMMAND ----------

for df, table in ((clean, silver_table), (quarantined, quarantine_table)):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"FILE_DATE = '{file_date}'")
        .partitionBy("FILE_DATE")
        .saveAsTable(table)
    )

rows_out = spark.table(silver_table).filter(F.col("FILE_DATE") == file_date).count()
rows_quarantined = (
    spark.table(quarantine_table).filter(F.col("FILE_DATE") == file_date).count()
)

log_run(
    spark,
    audit_table,
    "silver",
    file_date,
    rows_in=rows_in,
    rows_out=rows_out,
    rows_quarantined=rows_quarantined,
)
print(
    f"Silver complete for {file_date}: {rows_out} clean rows, "
    f"{rows_quarantined} quarantined"
)
