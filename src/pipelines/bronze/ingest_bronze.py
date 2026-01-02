from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.pipelines.config import Paths


def _write(df, path: str, fmt: str) -> None:
    if fmt == "delta":
        df.write.mode("overwrite").format("delta").save(path)
    else:
        df.write.mode("overwrite").parquet(path)


def run(spark: SparkSession, paths: Paths, fmt: str) -> None:
    base = paths.raw.rstrip("/")
    customers = spark.read.option("header", True).csv(f"{base}/customers.csv")
    accounts = spark.read.option("header", True).csv(f"{base}/accounts.csv")
    branches = spark.read.option("header", True).csv(f"{base}/branches.csv")
    txns = spark.read.option("header", True).csv(f"{base}/transactions.csv")

    _write(customers, f"{paths.bronze.rstrip('/')}/customers", fmt)
    _write(accounts, f"{paths.bronze.rstrip('/')}/accounts", fmt)
    _write(branches, f"{paths.bronze.rstrip('/')}/branches", fmt)
    _write(txns, f"{paths.bronze.rstrip('/')}/transactions", fmt)


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

