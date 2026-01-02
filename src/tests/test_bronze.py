import pytest
import os
import shutil
from src.pipelines.bronze import ingest_bronze
from src.pipelines.config import Paths

def test_ingest_bronze(spark, tmp_path):
    # Setup paths
    raw_dir = str(tmp_path / "raw")
    bronze_dir = str(tmp_path / "bronze")
    os.makedirs(raw_dir)
    
    paths = Paths(
        raw=raw_dir,
        bronze=bronze_dir,
        silver=str(tmp_path / "silver"),
        gold=str(tmp_path / "gold")
    )
    
    # Create sample CSVs
    spark.createDataFrame([("1", "John Doe")], ["customer_id", "full_name"]).write.csv(f"{raw_dir}/customers.csv", header=True)
    spark.createDataFrame([("1", "1", "B1", "savings", "USD", "2023-01-01", "active")], ["account_id", "customer_id", "branch_id", "account_type", "currency", "opened_at", "status"]).write.csv(f"{raw_dir}/accounts.csv", header=True)
    spark.createDataFrame([("B1", "Main St")], ["branch_id", "branch_name"]).write.csv(f"{raw_dir}/branches.csv", header=True)
    spark.createDataFrame([("T1", "1", "1", "2023-01-01 10:00:00", "deposit", "ATM", "100.0", "USD", "completed", "ref")], ["transaction_id", "account_id", "customer_id", "txn_ts", "txn_type", "merchant", "amount", "currency", "status", "reference"]).write.csv(f"{raw_dir}/transactions.csv", header=True)
    
    # Run pipeline
    ingest_bronze.run(spark, paths, "parquet")
    
    # Assert
    assert os.path.exists(f"{bronze_dir}/customers")
    assert spark.read.parquet(f"{bronze_dir}/customers").count() == 1
