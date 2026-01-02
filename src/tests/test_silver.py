import pytest
import os
from src.pipelines.silver import transform_silver
from src.pipelines.config import Paths

def test_transform_silver(spark, tmp_path):
    bronze_dir = str(tmp_path / "bronze")
    silver_dir = str(tmp_path / "silver")
    os.makedirs(bronze_dir)
    
    paths = Paths(
        raw=str(tmp_path / "raw"),
        bronze=bronze_dir,
        silver=silver_dir,
        gold=str(tmp_path / "gold")
    )
    
    # Create sample bronze data
    spark.createDataFrame([("1", "John Doe", "john@example.com", "123", "USA", "2023-01-01 10:00:00")], ["customer_id", "full_name", "email", "phone", "country", "created_at"]).write.parquet(f"{bronze_dir}/customers")
    spark.createDataFrame([("1", "1", "B1", "savings", "USD", "2023-01-01 10:00:00", "active")], ["account_id", "customer_id", "branch_id", "account_type", "currency", "opened_at", "status"]).write.parquet(f"{bronze_dir}/accounts")
    spark.createDataFrame([("B1", "Main St")], ["branch_id", "branch_name"]).write.parquet(f"{bronze_dir}/branches")
    spark.createDataFrame([("T1", "1", "1", "2023-01-01 10:00:00", "deposit", "ATM", "100.0", "USD", "completed", "ref")], ["transaction_id", "account_id", "customer_id", "txn_ts", "txn_type", "merchant", "amount", "currency", "status", "reference"]).write.parquet(f"{bronze_dir}/transactions")
    
    # Run pipeline
    transform_silver.run(spark, paths, "parquet")
    
    # Assert
    customers_s = spark.read.parquet(f"{silver_dir}/customers")
    assert "created_at_ts" in customers_s.columns
    assert customers_s.count() == 1
