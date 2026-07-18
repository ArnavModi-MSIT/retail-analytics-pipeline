from pyspark.sql import Row

from dim_product_dedup import build_dim_product


def test_picks_most_frequent_description_per_stock_code(spark):
    # StockCode "85048" appears 3x with one description, 1x with a typo variant.
    # Mode should win, not first/last-loaded.
    rows = [
        Row(StockCode="85048", Description="CHRISTMAS GLASS BALL"),
        Row(StockCode="85048", Description="CHRISTMAS GLASS BALL"),
        Row(StockCode="85048", Description="CHRISTMAS GLASS BALL"),
        Row(StockCode="85048", Description="CHRISTMAS GLASS BALL 20 LIGHTS"),
        Row(StockCode="22041", Description="RECORD FRAME 7\" SINGLE SIZE"),
    ]
    df = spark.createDataFrame(rows)

    result = build_dim_product(df).collect()
    result_map = {r["stock_code"]: r["description"] for r in result}

    assert result_map["85048"] == "CHRISTMAS GLASS BALL"
    assert result_map["22041"] == "RECORD FRAME 7\" SINGLE SIZE"
    assert len(result_map) == 2  # one row per distinct StockCode, not per description variant
