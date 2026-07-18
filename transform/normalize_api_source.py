"""
Normalizes the raw DummyJSON carts landing file into the same column shape as
schema_validation.py's `valid` bucket, so both sources can merge into fact_sales.

Documented limitations of the API source (by design, not oversight):
  - No real Country data -> defaulted to "Unknown"
  - No real Invoice number -> synthesized as "API-{cart_id}"
  - No StockCode -> API's numeric product_id cast to string, prefixed "API-"
    to guarantee no collision with real UK-source StockCodes
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def load_api_raw(spark, path: str) -> DataFrame:
    return spark.read.option("multiline", "true").json(path)


def normalize_api_source(df: DataFrame) -> DataFrame:
    return df.select(
        F.concat(F.lit("API-"), F.col("cart_id").cast("string")).alias("Invoice"),
        F.concat(F.lit("API-"), F.col("product_id").cast("string")).alias("StockCode"),
        F.col("title").alias("Description"),
        F.col("quantity").cast("int").alias("Quantity"),
        F.to_timestamp("fetched_at").alias("InvoiceDate"),
        F.round(F.col("price").cast("double"), 2).alias("Price"),
        F.col("user_id").cast("int").alias("CustomerID"),
        F.lit("Unknown").alias("Country"),
    )


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from schema_validation import classify_rows

    spark = SparkSession.builder.appName("normalize-api-source").getOrCreate()

    raw = load_api_raw(spark, "data/raw/api/carts_*.json")
    normalized = normalize_api_source(raw)
    buckets = classify_rows(normalized)

    print({name: b.count() for name, b in buckets.items()})

    buckets["valid"].write.mode("overwrite").parquet("data/staged/api_valid")
    buckets["quarantine"].write.mode("overwrite").parquet("data/staged/api_quarantine")
