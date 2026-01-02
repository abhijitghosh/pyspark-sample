from datetime import datetime
from src.pipelines.silver.transform_silver import transform_customers, transform_accounts, transform_transactions

def test_transform_customers(spark):
    input_data = [
        ("1", "John Doe", "john@example.com", "1234567890", "USA", "2023-01-01 10:00:00")
    ]
    input_schema = ["customer_id", "full_name", "email", "phone", "country", "created_at"]
    df = spark.createDataFrame(input_data, input_schema)

    result = transform_customers(df)

    assert result.count() == 1
    row = result.collect()[0]
    assert row.customer_id == "1"
    assert row.created_at_ts == datetime(2023, 1, 1, 10, 0, 0)
    assert "created_at" not in result.columns

def test_transform_accounts(spark):
    input_data = [
        ("A1", "C1", "B1", "savings", "USD", "2023-01-01 12:00:00", "active")
    ]
    input_schema = ["account_id", "customer_id", "branch_id", "account_type", "currency", "opened_at", "status"]
    df = spark.createDataFrame(input_data, input_schema)

    result = transform_accounts(df)

    assert result.count() == 1
    row = result.collect()[0]
    assert row.account_status == "active"
    assert row.opened_at_ts == datetime(2023, 1, 1, 12, 0, 0)
    assert "status" not in result.columns
    assert "opened_at" not in result.columns

def test_transform_transactions(spark):
    input_data = [
        ("T1", "A1", "C1", "2023-01-01 14:30:00", "debit", "Store", "100.50", "USD", "completed", "REF123")
    ]
    input_schema = ["transaction_id", "account_id", "customer_id", "txn_ts", "txn_type", "merchant", "amount", "currency", "status", "reference"]
    df = spark.createDataFrame(input_data, input_schema)

    result = transform_transactions(df)

    assert result.count() == 1
    row = result.collect()[0]
    assert row.amount == 100.50
    assert isinstance(row.amount, float)
    assert row.txn_ts == datetime(2023, 1, 1, 14, 30, 0)
    assert row.txn_status == "completed"
