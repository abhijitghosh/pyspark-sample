from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


def run(
    spark: SparkSession,
    raw_data_path: str,
    database: str = "main",
    schema: str = "demo_bronze"
) -> None:
    """
    Ingest retail raw CSVs into demo_bronze schema.

    Args:
        spark: SparkSession
        raw_data_path: Path to raw CSV files (e.g., /path/to/retail/)
        database: Database/catalog name (default: "main")
        schema: Bronze schema name (default: "demo_bronze")
    """
    base = raw_data_path.rstrip("/")
    full_schema = f"{database}.{schema}"

    # Read raw CSVs
    products = spark.read.option("header", True).csv(f"{base}/products.csv")
    orders = spark.read.option("header", True).csv(f"{base}/orders.csv")
    order_items = spark.read.option("header", True).csv(f"{base}/order_items.csv")

    # Write to unified bronze schema
    products.write.mode("overwrite").saveAsTable(f"{full_schema}.products")
    orders.write.mode("overwrite").saveAsTable(f"{full_schema}.orders")
    order_items.write.mode("overwrite").saveAsTable(f"{full_schema}.order_items")
