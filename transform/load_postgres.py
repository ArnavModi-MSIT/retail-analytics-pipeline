"""
Loads staged `valid` parquet (written by transform/schema_validation.py and
transform/normalize_api_source.py) into the Postgres star schema:
dim_product, dim_customer, dim_country, fact_sales.
dim_date is pre-populated separately (sql/populate_dim_date.sql) — not touched here.

Expects data/staged/valid and data/staged/api_valid to already exist —
run validate_schema and normalize_api (Airflow tasks) or their underlying
scripts first. Truncate-and-load for dims + fact on every run.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from dim_product_dedup import build_dim_product

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

JDBC_URL = os.environ.get(
    "JDBC_URL", "jdbc:postgresql://localhost:5432/retail_analytics"
)
JDBC_PROPERTIES = {
    "user": os.environ.get("PG_USER", "postgres"),
    "password": os.environ["PG_PASSWORD"],
    "driver": "org.postgresql.Driver",
}


def write_table(df: DataFrame, table: str, mode: str = "append") -> None:
    df.write.jdbc(url=JDBC_URL, table=table, mode=mode, properties=JDBC_PROPERTIES)


def read_table(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPERTIES)


def build_dim_country(valid_df: DataFrame) -> DataFrame:
    return (
        valid_df.select(F.col("Country").alias("country_name"))
        .distinct()
    )


def build_dim_customer(valid_df: DataFrame) -> DataFrame:
    return (
        valid_df.select(F.col("CustomerID").alias("customer_id"))
        .distinct()
    )


def build_fact_sales(valid_df: DataFrame, dim_product: DataFrame,
                      dim_customer: DataFrame, dim_country: DataFrame,
                      dim_date: DataFrame) -> DataFrame:
    df = (
        valid_df
        .withColumn("date_key", F.date_format("InvoiceDate", "yyyyMMdd").cast("int"))
        .withColumn("revenue", F.round(F.col("Quantity") * F.col("Price"), 2))
    )

    df = df.join(dim_product, df.StockCode == dim_product.stock_code, "inner")
    df = df.join(dim_customer, df.CustomerID == dim_customer.customer_id, "inner")
    df = df.join(dim_country, df.Country == dim_country.country_name, "inner")
    df = df.join(dim_date, df.date_key == dim_date.date_key, "inner")

    return df.select(
        F.col("Invoice").alias("invoice_no"),
        F.col("product_key"),
        F.col("customer_key"),
        dim_date.date_key,
        F.col("country_key"),
        F.col("InvoiceDate").alias("invoice_datetime"),
        F.col("Quantity").alias("quantity"),
        F.col("Price").alias("unit_price"),
        F.col("revenue"),
        F.col("source"),
    )


def truncate_tables():
    conn = psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        dbname=os.environ.get("PG_DB", "retail_analytics"),
        user=JDBC_PROPERTIES["user"],
        password=JDBC_PROPERTIES["password"],
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        # fact table first (FK dependency), then dims
        cur.execute(
            "TRUNCATE TABLE fact_sales, dim_product, dim_customer, "
            "dim_country RESTART IDENTITY CASCADE;"
        )
    conn.close()


def main():
    truncate_tables()

    spark = (
        SparkSession.builder
        .appName("retail-load-postgres")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

    csv_valid = spark.read.parquet("data/staged/valid").withColumn("source", F.lit("csv"))
    api_valid = spark.read.parquet("data/staged/api_valid").withColumn("source", F.lit("api"))

    valid_df = csv_valid.unionByName(api_valid)
    dim_product_df = build_dim_product(valid_df)
    write_table(dim_product_df, "dim_product")

    dim_customer_df = build_dim_customer(valid_df)
    write_table(dim_customer_df, "dim_customer")

    dim_country_df = build_dim_country(valid_df)
    write_table(dim_country_df, "dim_country")

    # --- Re-read dims to get generated surrogate keys ---
    dim_product = read_table(spark, "dim_product")
    dim_customer = read_table(spark, "dim_customer")
    dim_country = read_table(spark, "dim_country")
    dim_date = read_table(spark, "dim_date").select("date_key")

    # --- Fact ---
    fact_df = build_fact_sales(valid_df, dim_product, dim_customer, dim_country, dim_date)
    write_table(fact_df, "fact_sales")

    print(f"Loaded: dim_product={dim_product_df.count()}, "
          f"dim_customer={dim_customer_df.count()}, "
          f"dim_country={dim_country_df.count()}, "
          f"fact_sales={fact_df.count()}")


if __name__ == "__main__":
    main()
