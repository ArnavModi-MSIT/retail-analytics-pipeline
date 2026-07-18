import os
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType,
)

# Pipeline modules live in transform/ and ingestion/, not a package —
# add them to sys.path so tests can import directly, same as the DAG does.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "transform"))
sys.path.insert(0, os.path.join(ROOT, "ingestion"))


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("retail-pipeline-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


# Matches schema_validation.cast_types() output — used so test DataFrames with
# None values don't hit Spark's "cannot infer schema from all-null column" error.
TYPED_SCHEMA = StructType([
    StructField("Invoice", StringType(), nullable=False),
    StructField("StockCode", StringType(), nullable=False),
    StructField("Description", StringType(), nullable=True),
    StructField("Quantity", IntegerType(), nullable=False),
    StructField("InvoiceDate", TimestampType(), nullable=True),
    StructField("Price", DoubleType(), nullable=False),
    StructField("CustomerID", IntegerType(), nullable=True),
    StructField("Country", StringType(), nullable=True),
])
