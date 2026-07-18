from pyspark.sql import Row

from normalize_api_source import normalize_api_source


def test_maps_dummyjson_columns_to_csv_source_schema(spark):
    rows = [
        Row(
            cart_id=1,
            user_id=42,
            product_id=101,
            title="Essence Mascara Lash Princess",
            price=9.99,
            quantity=2,
            discount_percentage=7.17,
            discounted_total=18.55,
            fetched_at="2026-07-14T10:00:00+00:00",
        )
    ]
    df = spark.createDataFrame(rows)

    result = normalize_api_source(df).collect()[0]

    # Synthetic keys prefixed to guarantee no collision with real CSV-source keys
    assert result["Invoice"] == "API-1"
    assert result["StockCode"] == "API-101"
    assert result["Description"] == "Essence Mascara Lash Princess"
    assert result["Quantity"] == 2
    assert result["Price"] == 9.99
    assert result["CustomerID"] == 42
    assert result["Country"] == "Unknown"  # documented fabricated default
