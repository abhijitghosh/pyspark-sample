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
    Transform banking bronze tables into silver layer.

    Args:
        spark: SparkSession
        database: Database/catalog name (default: "main")
        bronze_schema: Bronze schema name (default: "demo_bronze")
        silver_schema: Silver schema name (default: "demo_silver")
    """
    bronze = f"{database}.{bronze_schema}"
    silver = f"{database}.{silver_schema}"

    # Read from bronze
    customers = spark.table(f"{bronze}.customers").dropDuplicates(["customer_id"])
    accounts = spark.table(f"{bronze}.accounts").dropDuplicates(["account_id"])
    branches = spark.table(f"{bronze}.branches").dropDuplicates(["branch_id"])
    txns = spark.table(f"{bronze}.transactions")

    # Transform customers
    customers_s = customers.select(
        col("customer_id"),
        col("full_name"),
        col("email"),
        col("phone"),
        col("country"),
        to_timestamp(col("created_at")).alias("created_at_ts"),
    )

    # Transform accounts
    accounts_s = accounts.select(
        col("account_id"),
        col("customer_id"),
        col("branch_id"),
        col("account_type"),
        col("currency"),
        to_timestamp(col("opened_at")).alias("opened_at_ts"),
        col("status").alias("account_status"),
    )

    # Transform transactions
    txns_s = txns.select(
        col("transaction_id"),
        col("account_id"),
        col("customer_id"),
        to_timestamp(col("txn_ts")).alias("txn_ts"),
        col("txn_type"),
        col("merchant"),
        col("amount").cast("double").alias("amount"),
        col("currency"),
        col("status").alias("txn_status"),
        col("reference"),
    )

    # Write to silver
    customers_s.write.mode("overwrite").saveAsTable(f"{silver}.banking_customers")
    accounts_s.write.mode("overwrite").saveAsTable(f"{silver}.banking_accounts")
    branches.write.mode("overwrite").saveAsTable(f"{silver}.banking_branches")
    txns_s.write.mode("overwrite").saveAsTable(f"{silver}.banking_transactions")
