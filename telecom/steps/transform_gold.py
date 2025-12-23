from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as fsum, to_date, avg


def run(
    spark: SparkSession,
    database: str = "main",
    silver_schema: str = "demo_silver",
    gold_schema: str = "demo_gold"
) -> None:
    """
    Create telecom gold layer metrics.

    Args:
        spark: SparkSession
        database: Database/catalog name (default: "main")
        silver_schema: Silver schema name (default: "demo_silver")
        gold_schema: Gold schema name (default: "demo_gold")
    """
    silver = f"{database}.{silver_schema}"
    gold = f"{database}.{gold_schema}"

    # Read from silver
    subscribers = spark.table(f"{silver}.telecom_subscribers")
    plans = spark.table(f"{silver}.telecom_plans")
    cdr_usage = spark.table(f"{silver}.telecom_cdr_usage")

    # Daily usage metrics
    daily = (
        cdr_usage
        .withColumn("event_date", to_date(col("event_ts")))
        .groupBy("event_date", "event_type")
        .agg(
            count("*").alias("event_count"),
            fsum(col("data_mb")).alias("total_data_mb"),
            fsum(col("duration_sec")).alias("total_duration_sec"),
        )
    )

    # Subscriber usage summary
    subscriber_usage = (
        cdr_usage
        .groupBy("subscriber_id")
        .agg(
            count("*").alias("event_count"),
            fsum(col("data_mb")).alias("total_data_mb"),
            fsum(col("duration_sec")).alias("total_voice_sec"),
        )
        .join(subscribers.select("subscriber_id", "plan_id"), "subscriber_id")
    )

    # Plan adoption metrics
    plan_adoption = (
        subscribers
        .groupBy("plan_id")
        .agg(count("*").alias("subscriber_count"))
        .join(plans, "plan_id")
        .select(
            "plan_id",
            "plan_name",
            "monthly_fee",
            "subscriber_count",
        )
        .orderBy(col("subscriber_count").desc())
    )

    # Write to gold
    daily.write.mode("overwrite").saveAsTable(f"{gold}.telecom_daily_usage")
    subscriber_usage.write.mode("overwrite").saveAsTable(f"{gold}.telecom_subscriber_usage")
    plan_adoption.write.mode("overwrite").saveAsTable(f"{gold}.telecom_plan_adoption")
