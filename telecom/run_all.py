from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from src.code.telecom.pyspark.steps import ingest_bronze, transform_gold, transform_silver


def main() -> int:
    """
    Run full telecom pipeline: Bronze → Silver → Gold.

    Example usage:
        python -m src.code.telecom.pyspark.run_all \
            --raw-path /path/to/telecom/csvs \
            --database main \
            --bronze demo_bronze \
            --silver demo_silver \
            --gold demo_gold
    """
    ap = argparse.ArgumentParser(description="Telecom PySpark Pipeline")
    ap.add_argument("--raw-path", required=True, help="Path to raw CSV files")
    ap.add_argument("--database", default="main", help="Database/catalog name")
    ap.add_argument("--bronze", default="demo_bronze", help="Bronze schema name")
    ap.add_argument("--silver", default="demo_silver", help="Silver schema name")
    ap.add_argument("--gold", default="demo_gold", help="Gold schema name")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("telecom-pipeline").getOrCreate()

    print(f"[1/3] Ingesting bronze layer from {args.raw_path}...")
    ingest_bronze.run(spark, args.raw_path, args.database, args.bronze)

    print(f"[2/3] Transforming silver layer...")
    transform_silver.run(spark, args.database, args.bronze, args.silver)

    print(f"[3/3] Creating gold metrics...")
    transform_gold.run(spark, args.database, args.silver, args.gold)

    print("✓ Telecom pipeline completed successfully!")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
