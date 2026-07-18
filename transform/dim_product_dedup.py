"""
dim_product source-side dedup logic.
Picks the most frequent Description per StockCode (mode), not first/last-loaded,
since Description varies for the same StockCode in this dataset (data quality issue).
Call this on the `valid` bucket before loading into dim_product.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_dim_product(valid_df: DataFrame) -> DataFrame:
    counted = (
        valid_df.groupBy("StockCode", "Description")
        .agg(F.count("*").alias("desc_count"))
    )

    rank_window = Window.partitionBy("StockCode").orderBy(F.desc("desc_count"))

    ranked = counted.withColumn("rn", F.row_number().over(rank_window))

    return (
        ranked.filter(F.col("rn") == 1)
        .select("StockCode", "Description")
        .withColumnRenamed("StockCode", "stock_code")
        .withColumnRenamed("Description", "description")
    )
