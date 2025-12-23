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
    Transform telecom bronze tables into silver layer.

    Args:
        spark: SparkSession
        database: Database/catalog name (default: "main")
        bronze_schema: Bronze schema name (default: "demo_bronze")
        silver_schema: Silver schema name (default: "demo_silver")
    """
    bronze = f"{database}.{bronze_schema}"
    silver = f"{database}.{silver_schema}"

    # Read from bronze
    subscribers = spark.table(f"{bronze}.subscribers").dropDuplicates(["subscriber_id"])
    plans = spark.table(f"{bronze}.plans").dropDuplicates(["plan_id"])
    cdr_usage = spark.table(f"{bronze}.cdr_usage")
    bills = spark.table(f"{bronze}.bills").dropDuplicates(["bill_id"])

    # Transform subscribers
    subscribers_s = subscribers.select(
        col("subscriber_id"),
        col("msisdn"),
        col("country"),
        col("plan_id"),
        to_timestamp(col("activated_at")).alias("activated_at_ts"),
        col("status").alias("subscriber_status"),
    )

    # Transform plans
    plans_s = plans.select(
        col("plan_id"),
        col("plan_name"),
        col("monthly_fee").cast("double").alias("monthly_fee"),
        col("data_gb").cast("int").alias("data_gb"),
        col("voice_minutes").cast("int").alias("voice_minutes"),
        col("sms").cast("int").alias("sms"),
        col("country"),
    )

    # Transform CDR usage
    cdr_s = cdr_usage.select(
        col("cdr_id"),
        col("subscriber_id"),
        to_timestamp(col("event_ts")).alias("event_ts"),
        col("event_type"),
        col("duration_sec").cast("int").alias("duration_sec"),
        col("data_mb").cast("double").alias("data_mb"),
        col("cell_id"),
        col("roaming").cast("int").alias("roaming"),
        col("status").alias("cdr_status"),
    )

    # Transform bills
    bills_s = bills.select(
        col("bill_id"),
        col("subscriber_id"),
        col("bill_month"),
        col("amount_due").cast("double").alias("amount_due"),
        col("amount_paid").cast("double").alias("amount_paid"),
        to_timestamp(col("paid_at")).alias("paid_at_ts"),
        col("status").alias("bill_status"),
    )

    # Write to silver
    subscribers_s.write.mode("overwrite").saveAsTable(f"{silver}.telecom_subscribers")
    plans_s.write.mode("overwrite").saveAsTable(f"{silver}.telecom_plans")
    cdr_s.write.mode("overwrite").saveAsTable(f"{silver}.telecom_cdr_usage")
    bills_s.write.mode("overwrite").saveAsTable(f"{silver}.telecom_bills")
