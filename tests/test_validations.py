import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from src.etl.validations import filter_valid_transactions


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .appName("Testing")
        .getOrCreate()
    )


def test_filter_valid_transactions_null_amounts(spark):
    """Test filter_valid_transactions — removes null amounts."""
    input_data = [("T1", "2020-01-01", 100.0), ("T2", "2020-01-01", None)]
    input_df = spark.createDataFrame(input_data, ["transaction_id", "date", "amount"])

    expected_data = [("T1", "2020-01-01", 100.0)]
    expected_df = spark.createDataFrame(expected_data, ["transaction_id", "date", "amount"])

    result_df = filter_valid_transactions(input_df)
    assert_df_equality(result_df, expected_df, ignore_nullable=True)


def test_filter_valid_transactions_date_range(spark):
    """Test filter_valid_transactions — removes out-of-range dates."""
    input_data = [
        ("T1", "2020-01-01", 100.0),
        ("T2", "2010-05-15", 50.0),
        ("T3", "2026-01-01", 20.0),
    ]
    input_df = spark.createDataFrame(input_data, ["transaction_id", "date", "amount"])

    expected_data = [("T1", "2020-01-01", 100.0)]
    expected_df = spark.createDataFrame(expected_data, ["transaction_id", "date", "amount"])

    result_df = filter_valid_transactions(input_df)
    assert_df_equality(result_df, expected_df, ignore_nullable=True)


def test_filter_valid_transactions_keep_refunds(spark):
    """Test filter_valid_transactions — keeps refunds (negative amounts)."""
    input_data = [("T1", "2020-01-01", 100.0), ("T2", "2020-01-01", -25.0)]
    input_df = spark.createDataFrame(input_data, ["transaction_id", "date", "amount"])

    expected_data = [("T1", "2020-01-01", 100.0), ("T2", "2020-01-01", -25.0)]
    expected_df = spark.createDataFrame(expected_data, ["transaction_id", "date", "amount"])

    result_df = filter_valid_transactions(input_df)
    assert_df_equality(result_df, expected_df, ignore_nullable=True)
