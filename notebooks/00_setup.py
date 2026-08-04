# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: schema, volume and audit table
# MAGIC
# MAGIC Idempotent one-time setup (safe to re-run). Creates:
# MAGIC - the target **schema** (database) inside the chosen catalog,
# MAGIC - a **landing volume** where daily files arrive under `landing/<yyyy-mm-dd>/`,
# MAGIC - the **run-audit table** used for monitoring every pipeline stage.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "transactions")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`landing`")

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.etl_run_log (
        run_ts            TIMESTAMP COMMENT 'When the stage finished (UTC)',
        stage             STRING    COMMENT 'bronze / silver / gold',
        file_date         STRING    COMMENT 'File date the run processed',
        rows_in           BIGINT,
        rows_out          BIGINT,
        rows_quarantined  BIGINT,
        notes             STRING
    )
    COMMENT 'One row per pipeline stage per run - the monitoring backbone'
    """
)

print(f"Setup complete for {catalog}.{schema}")
