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
    Create banking gold layer metrics.

    Args:
        spark: SparkSession
        database: Database/catalog name (default: "main")
        silver_schema: Silver schema name (default: "demo_silver")
        gold_schema: Gold schema name (default: "demo_gold")
    """
    silver = f"{database}.{silver_schema}"
    gold = f"{database}.{gold_schema}"

    # Read from silver
    txns = spark.table(f"{silver}.banking_transactions")

    # Daily transaction metrics
    daily = (
        txns.withColumn("txn_date", to_date(col("txn_ts")))
        .groupBy("txn_date", "currency", "txn_type")
        .agg(
            count("*").alias("txn_count"),
            fsum(col("amount")).alias("total_amount"),
        )
    )

    # Customer value metrics
    by_customer = (
        txns.groupBy("customer_id")
        .agg(
            count("*").alias("txn_count"),
            fsum(col("amount")).alias("net_amount"),
        )
        .orderBy(col("net_amount").desc())
    )

    # Write to gold
    daily.write.mode("overwrite").saveAsTable(f"{gold}.banking_daily_txn_metrics")
    by_customer.write.mode("overwrite").saveAsTable(f"{gold}.banking_customer_value")
