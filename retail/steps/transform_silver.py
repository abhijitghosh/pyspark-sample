from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp


def run(
    spark: SparkSession,
    database: str = "main",
    bronze_schema: str = "demo_bronze",
    silver_schema: str = "demo_silver"
) -> None:
    """
    Transform retail bronze tables into silver layer.

    Args:
        spark: SparkSession
        database: Database/catalog name (default: "main")
        bronze_schema: Bronze schema name (default: "demo_bronze")
        silver_schema: Silver schema name (default: "demo_silver")
    """
    bronze = f"{database}.{bronze_schema}"
    silver = f"{database}.{silver_schema}"

    # Read from bronze
    products = spark.table(f"{bronze}.products").dropDuplicates(["product_id"])
    orders = spark.table(f"{bronze}.orders").dropDuplicates(["order_id"])
    order_items = spark.table(f"{bronze}.order_items").dropDuplicates(["order_item_id"])

    # Transform products
    products_s = products.select(
        col("product_id"),
        col("product_name"),
        col("category"),
        col("unit_price").cast("double").alias("unit_price"),
        col("currency"),
        col("is_active").cast("int").alias("is_active"),
    )

    # Transform orders
    orders_s = orders.select(
        col("order_id"),
        col("customer_id"),
        to_timestamp(col("order_ts")).alias("order_ts"),
        col("channel"),
        col("country"),
        col("status").alias("order_status"),
        col("promo_code"),
    )

    # Transform order items
    order_items_s = order_items.select(
        col("order_item_id"),
        col("order_id"),
        col("product_id"),
        col("qty").cast("int").alias("qty"),
        col("unit_price").cast("double").alias("unit_price"),
        col("discount").cast("double").alias("discount"),
        col("line_total").cast("double").alias("line_total"),
    )

    # Write to silver
    products_s.write.mode("overwrite").saveAsTable(f"{silver}.retail_products")
    orders_s.write.mode("overwrite").saveAsTable(f"{silver}.retail_orders")
    order_items_s.write.mode("overwrite").saveAsTable(f"{silver}.retail_order_items")
