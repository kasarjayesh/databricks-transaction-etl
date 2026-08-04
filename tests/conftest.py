"""Shared pytest fixtures: one local SparkSession for the whole test run."""

import os
import sys

# Environment must be set BEFORE pyspark is imported: pyspark reads these
# during import to locate SPARK_HOME and the worker Python.
# - On Windows, Spark's default worker command is 'python3', which does not
#   exist; point driver and workers at the interpreter running the tests.
# - SPARK_LOCAL_IP=127.0.0.1 stops Spark resolving the machine hostname (which
#   Docker Desktop redirects to host.docker.internal) for its local sockets.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

import pytest  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("transaction-etl-tests")
        # Tiny data: 2 shuffle partitions instead of the default 200.
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        # If a Python worker crashes, capture its real traceback in the logs.
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .getOrCreate()
    )
    yield session
    session.stop()
