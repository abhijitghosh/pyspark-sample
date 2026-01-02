from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as fsum, to_date

from src.support.mock.pyspark_common import Paths


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

