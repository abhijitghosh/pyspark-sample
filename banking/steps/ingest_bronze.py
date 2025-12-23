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
    Ingest banking raw CSVs into demo_bronze schema.

    Args:
        spark: SparkSession
        raw_data_path: Path to raw CSV files (e.g., /path/to/banking/)
        database: Database/catalog name (default: "main")
        schema: Bronze schema name (default: "demo_bronze")
    """
    base = raw_data_path.rstrip("/")
    full_schema = f"{database}.{schema}"

    # Read raw CSVs
    customers = spark.read.option("header", True).csv(f"{base}/customers.csv")
    accounts = spark.read.option("header", True).csv(f"{base}/accounts.csv")
    branches = spark.read.option("header", True).csv(f"{base}/branches.csv")
    txns = spark.read.option("header", True).csv(f"{base}/transactions.csv")

    # Write to unified bronze schema
    customers.write.mode("overwrite").saveAsTable(f"{full_schema}.customers")
    accounts.write.mode("overwrite").saveAsTable(f"{full_schema}.accounts")
    branches.write.mode("overwrite").saveAsTable(f"{full_schema}.branches")
    txns.write.mode("overwrite").saveAsTable(f"{full_schema}.transactions")

