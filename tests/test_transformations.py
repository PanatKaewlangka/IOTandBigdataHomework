import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from chispa.dataframe_comparer import assert_df_equality
from src.etl.transformations import categorize_spending, enrich_with_lookups


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .appName("Testing")
        .getOrCreate()
    )


def test_categorize_spending_basic(spark):
    """This test is provided as a starting point. It will FAIL until you implement categorize_spending."""
    input_data = [(5.0,), (25.0,), (100.0,), (500.0,)]
    input_df = spark.createDataFrame(input_data, ["amount"])

    expected_data = [
        (5.0, "micro"),
        (25.0, "small"),
        (100.0, "medium"),
        (500.0, "large"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["amount", "spending_tier"])

    result_df = categorize_spending(input_df)
    assert_df_equality(result_df, expected_df, ignore_nullable=True)


def test_categorize_spending_refunds(spark):
    """Test categorize_spending with negative amounts (refunds)."""
    input_data = [(-5.0,), (-25.0,), (-100.0,), (-500.0,)]
    input_df = spark.createDataFrame(input_data, ["amount"])

    expected_data = [
        (-5.0, "micro"),
        (-25.0, "small"),
        (-100.0, "medium"),
        (-500.0, "large"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["amount", "spending_tier"])

    result_df = categorize_spending(input_df)
    assert_df_equality(result_df, expected_df, ignore_nullable=True)


def test_categorize_spending_boundaries(spark):
    """Test categorize_spending with exact boundary values."""
    input_data = [(10.0,), (50.0,), (200.0,)]
    input_df = spark.createDataFrame(input_data, ["amount"])

    expected_data = [
        (10.0, "small"),
        (50.0, "medium"),
        (200.0, "large"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["amount", "spending_tier"])

    result_df = categorize_spending(input_df)
    assert_df_equality(result_df, expected_df, ignore_nullable=True)


def test_enrich_with_lookups_basic(spark):
    """Test enrich_with_lookups join logic."""
    # Transactions
    txn_data = [
        ("T1", "C1", "M1"),
        ("T2", "C2", "M2"),
    ]
    txn_df = spark.createDataFrame(txn_data, ["transaction_id", "category_id", "merchant_id"])

    # Categories
    cat_data = [
        ("C1", "Groceries", "Variable"),
        ("C2", "Rent", "Fixed"),
    ]
    cat_df = spark.createDataFrame(cat_data, ["category_id", "category_name", "budget_type"])

    # Merchants
    merch_data = [
        ("M1", "Walmart", "Supermarket"),
        ("M2", "Landlord", "Service"),
    ]
    merch_df = spark.createDataFrame(merch_data, ["merchant_id", "merchant_name", "merchant_type"])

    expected_data = [
        ("C1", "M1", "T1", "Groceries", "Variable", "Walmart", "Supermarket"),
        ("C2", "M2", "T2", "Rent", "Fixed", "Landlord", "Service"),
    ]
    # Note: Spark join might reorder columns, adjust expected accordingly or use select
    result_df = enrich_with_lookups(txn_df, cat_df, merch_df)
    
    # Check if essential columns exist and have correct values
    assert "category_name" in result_df.columns
    assert "merchant_name" in result_df.columns
    assert result_df.filter(F.col("transaction_id") == "T1").select("category_name").collect()[0][0] == "Groceries"
