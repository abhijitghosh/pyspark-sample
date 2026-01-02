from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as fsum, to_date

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


def run(spark: SparkSession, paths: Paths, fmt: str) -> None:
    s = paths.silver.rstrip("/")
    txns = _read(spark, f"{s}/transactions", fmt)

    daily = (
        txns.withColumn("txn_date", to_date(col("txn_ts")))
        .groupBy("txn_date", "currency", "txn_type")
        .agg(
            count("*").alias("txn_count"),
            fsum(col("amount")).alias("total_amount"),
        )
    )

    by_customer = (
        txns.groupBy("customer_id")
        .agg(
            count("*").alias("txn_count"),
            fsum(col("amount")).alias("net_amount"),
        )
        .orderBy(col("net_amount").desc())
    )

    g = paths.gold.rstrip("/")
    _write(daily, f"{g}/daily_txn_metrics", fmt)
    _write(by_customer, f"{g}/customer_value_metrics", fmt)


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

