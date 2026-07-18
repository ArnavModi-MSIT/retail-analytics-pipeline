"""
Schema validation for Online Retail II ingestion.
Buckets raw rows into: valid | returns | quarantine.
No silent drops — every row is accounted for downstream.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

RAW_SCHEMA = StructType([
    StructField("Invoice", StringType(), nullable=False),
    StructField("StockCode", StringType(), nullable=False),
    StructField("Description", StringType(), nullable=True),
    StructField("Quantity", IntegerType(), nullable=False),
    StructField("InvoiceDate", StringType(), nullable=False),  # cast explicitly, don't trust inference
    StructField("Price", DoubleType(), nullable=False),
    StructField("Customer ID", DoubleType(), nullable=True),   # float in source; cast to int after null-check
    StructField("Country", StringType(), nullable=True),
])


def load_raw(spark, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=RAW_SCHEMA)


def cast_types(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"))
          .withColumn("CustomerID", F.col("Customer ID").cast(IntegerType()))
          .drop("Customer ID")
    )


def classify_rows(df: DataFrame) -> dict[str, DataFrame]:
    """
    Split into three buckets based on observed data quality issues:
      - returns: Invoice starts with 'C' (cancellations/returns)
      - quarantine: negative price on non-return rows, null CustomerID,
                    null Description, or null InvoiceDate (failed cast)
      - valid: everything else
    """
    df = df.withColumn("is_return", F.col("Invoice").startswith("C"))

    returns_df = df.filter(F.col("is_return"))

    non_return_df = df.filter(~F.col("is_return"))

    quarantine_cond = (
        (F.col("Price") < 0) |
        F.col("CustomerID").isNull() |
        F.col("Description").isNull() |
        F.col("InvoiceDate").isNull()
    )

    quarantine_df = non_return_df.filter(quarantine_cond)
    valid_df = non_return_df.filter(~quarantine_cond)

    return {"valid": valid_df, "returns": returns_df, "quarantine": quarantine_df}


def validation_summary(buckets: dict[str, DataFrame]) -> dict[str, int]:
    return {name: df.count() for name, df in buckets.items()}


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("retail-schema-validation").getOrCreate()

    raw = load_raw(spark, "data/raw/online_retail_ii.csv")
    typed = cast_types(raw)
    buckets = classify_rows(typed)

    print(validation_summary(buckets))

    buckets["valid"].write.mode("overwrite").parquet("data/staged/valid")
    buckets["returns"].write.mode("overwrite").parquet("data/staged/returns")
    buckets["quarantine"].write.mode("overwrite").parquet("data/staged/quarantine")
