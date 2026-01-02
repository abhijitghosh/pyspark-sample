from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.support.mock.pyspark_common import Paths


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

