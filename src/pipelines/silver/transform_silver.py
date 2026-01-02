from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_timestamp

from src.pipelines.config import Paths


def _read(spark: SparkSession, path: str, fmt: str):
    if fmt == "delta":
        return spark.read.format("delta").load(path)
    return spark.read.parquet(path)


def _write(df, path: str, fmt: str) -> None:
    if fmt == "delta":
        df.write.mode("overwrite").format("delta").save(path)
    else:
        df.write.mode("overwrite").parquet(path)


def transform_customers(customers: DataFrame) -> DataFrame:
    return customers.select(
        col("customer_id"),
        col("full_name"),
        col("email"),
        col("phone"),
        col("country"),
        to_timestamp(col("created_at")).alias("created_at_ts"),
    )


def transform_accounts(accounts: DataFrame) -> DataFrame:
    return accounts.select(
        col("account_id"),
        col("customer_id"),
        col("branch_id"),
        col("account_type"),
        col("currency"),
        to_timestamp(col("opened_at")).alias("opened_at_ts"),
        col("status").alias("account_status"),
    )


def transform_transactions(txns: DataFrame) -> DataFrame:
    return txns.select(
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


def run(spark: SparkSession, paths: Paths, fmt: str) -> None:
    b = paths.bronze.rstrip("/")
    customers = _read(spark, f"{b}/customers", fmt).dropDuplicates(["customer_id"])
    accounts = _read(spark, f"{b}/accounts", fmt).dropDuplicates(["account_id"])
    branches = _read(spark, f"{b}/branches", fmt).dropDuplicates(["branch_id"])
    txns = _read(spark, f"{b}/transactions", fmt)

    customers_s = transform_customers(customers)
    accounts_s = transform_accounts(accounts)
    txns_s = transform_transactions(txns)

    s = paths.silver.rstrip("/")
    _write(customers_s, f"{s}/customers", fmt)
    _write(accounts_s, f"{s}/accounts", fmt)
    _write(branches, f"{s}/branches", fmt)
    _write(txns_s, f"{s}/transactions", fmt)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", default="/tmp/raw")
    parser.add_argument("--bronze", default="/tmp/bronze")
    parser.add_argument("--silver", default="/tmp/silver")
    parser.add_argument("--gold", default="/tmp/gold")
    parser.add_argument("--fmt", default="delta")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    paths = Paths(raw=args.raw, bronze=args.bronze, silver=args.silver, gold=args.gold)
    run(spark, paths, args.fmt)

