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
    Ingest telecom raw CSVs into demo_bronze schema.

    Args:
        spark: SparkSession
        raw_data_path: Path to raw CSV files (e.g., /path/to/telecom/)
        database: Database/catalog name (default: "main")
        schema: Bronze schema name (default: "demo_bronze")
    """
    base = raw_data_path.rstrip("/")
    full_schema = f"{database}.{schema}"

    # Read raw CSVs
    subscribers = spark.read.option("header", True).csv(f"{base}/subscribers.csv")
    plans = spark.read.option("header", True).csv(f"{base}/plans.csv")
    cdr_usage = spark.read.option("header", True).csv(f"{base}/cdr_usage.csv")
    bills = spark.read.option("header", True).csv(f"{base}/bills.csv")

    # Write to unified bronze schema
    subscribers.write.mode("overwrite").saveAsTable(f"{full_schema}.subscribers")
    plans.write.mode("overwrite").saveAsTable(f"{full_schema}.plans")
    cdr_usage.write.mode("overwrite").saveAsTable(f"{full_schema}.cdr_usage")
    bills.write.mode("overwrite").saveAsTable(f"{full_schema}.bills")
