import pytest
import os
from src.pipelines.gold import transform_gold
from src.pipelines.config import Paths
from pyspark.sql.functions import to_timestamp

def test_transform_gold(spark, tmp_path):
    silver_dir = str(tmp_path / "silver")
    gold_dir = str(tmp_path / "gold")
    os.makedirs(silver_dir)
    
    paths = Paths(
        raw=str(tmp_path / "raw"),
        bronze=str(tmp_path / "bronze"),
        silver=silver_dir,
        gold=gold_dir
    )
    
    # Create sample silver data
    txns = spark.createDataFrame([
        ("T1", "1", "1", "2023-01-01 10:00:00", "deposit", "ATM", 100.0, "USD", "completed", "ref"),
        ("T2", "1", "1", "2023-01-01 11:00:00", "deposit", "ATM", 50.0, "USD", "completed", "ref2")
    ], ["transaction_id", "account_id", "customer_id", "txn_ts_str", "txn_type", "merchant", "amount", "currency", "status", "reference"])
    
    txns = txns.withColumn("txn_ts", to_timestamp("txn_ts_str")).drop("txn_ts_str")
    txns.write.parquet(f"{silver_dir}/transactions")
    
    # Run pipeline
    transform_gold.run(spark, paths, "parquet")
    
    # Assert
    daily = spark.read.parquet(f"{gold_dir}/daily_txn_metrics")
    assert daily.count() == 1
    assert daily.collect()[0]["total_amount"] == 150.0
