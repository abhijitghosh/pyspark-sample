from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as fsum, to_date


def run(
    spark: SparkSession,
    database: str = "main",
    silver_schema: str = "demo_silver",
    gold_schema: str = "demo_gold"
) -> None:
    """
    Create retail gold layer metrics.

    Args:
        spark: SparkSession
        database: Database/catalog name (default: "main")
        silver_schema: Silver schema name (default: "demo_silver")
        gold_schema: Gold schema name (default: "demo_gold")
    """
    silver = f"{database}.{silver_schema}"
    gold = f"{database}.{gold_schema}"

    # Read from silver
    orders = spark.table(f"{silver}.retail_orders")
    order_items = spark.table(f"{silver}.retail_order_items")
    products = spark.table(f"{silver}.retail_products")

    # Join orders with items and products
    sales = (
        order_items
        .join(orders, "order_id")
        .join(products, "product_id")
    )

    # Daily sales metrics
    daily = (
        sales
        .withColumn("order_date", to_date(col("order_ts")))
        .groupBy("order_date", "category", "channel")
        .agg(
            count("*").alias("order_count"),
            fsum(col("line_total")).alias("total_revenue"),
        )
    )

    # Product performance metrics
    product_perf = (
        order_items
        .join(products, "product_id")
        .groupBy("product_id", "product_name", "category")
        .agg(
            count("*").alias("units_sold"),
            fsum(col("line_total")).alias("total_revenue"),
        )
        .orderBy(col("total_revenue").desc())
    )

    # Write to gold
    daily.write.mode("overwrite").saveAsTable(f"{gold}.retail_daily_sales")
    product_perf.write.mode("overwrite").saveAsTable(f"{gold}.retail_product_performance")
