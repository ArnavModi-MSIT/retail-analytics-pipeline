from datetime import datetime

from pyspark.sql import Row

from schema_validation import classify_rows
from conftest import TYPED_SCHEMA


def make_row(**overrides):
    defaults = dict(
        Invoice="536365",
        StockCode="85048",
        Description="CHRISTMAS GLASS BALL",
        Quantity=12,
        InvoiceDate=datetime(2010, 9, 21, 10, 47),
        Price=6.95,
        CustomerID=13085,
        Country="United Kingdom",
    )
    defaults.update(overrides)
    return Row(**defaults)


def test_valid_row_passes_through(spark):
    df = spark.createDataFrame([make_row()], schema=TYPED_SCHEMA)
    buckets = classify_rows(df)

    assert buckets["valid"].count() == 1
    assert buckets["returns"].count() == 0
    assert buckets["quarantine"].count() == 0


def test_invoice_starting_with_c_is_a_return(spark):
    df = spark.createDataFrame([make_row(Invoice="C536379")], schema=TYPED_SCHEMA)
    buckets = classify_rows(df)

    assert buckets["returns"].count() == 1
    assert buckets["valid"].count() == 0
    assert buckets["quarantine"].count() == 0


def test_negative_price_on_non_return_is_quarantined(spark):
    df = spark.createDataFrame([make_row(Price=-11.62)], schema=TYPED_SCHEMA)
    buckets = classify_rows(df)

    assert buckets["quarantine"].count() == 1
    assert buckets["valid"].count() == 0


def test_null_customer_id_is_quarantined(spark):
    df = spark.createDataFrame([make_row(CustomerID=None)], schema=TYPED_SCHEMA)
    buckets = classify_rows(df)

    assert buckets["quarantine"].count() == 1
    assert buckets["valid"].count() == 0


def test_null_description_is_quarantined(spark):
    df = spark.createDataFrame([make_row(Description=None)], schema=TYPED_SCHEMA)
    buckets = classify_rows(df)

    assert buckets["quarantine"].count() == 1
    assert buckets["valid"].count() == 0


def test_bucket_counts_sum_to_total_input(spark):
    rows = [
        make_row(),
        make_row(Invoice="C536379"),
        make_row(Price=-5.0),
        make_row(CustomerID=None),
    ]
    df = spark.createDataFrame(rows, schema=TYPED_SCHEMA)
    buckets = classify_rows(df)

    total = sum(b.count() for b in buckets.values())
    assert total == len(rows)
