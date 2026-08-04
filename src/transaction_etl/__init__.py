"""Transaction ETL: reusable transformation logic for the Databricks pipeline.

The heavy lifting lives in this package (testable plain Python); the
Databricks notebooks are thin wrappers that call into it.
"""
