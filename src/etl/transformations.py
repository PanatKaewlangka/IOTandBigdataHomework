from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def categorize_spending(df: DataFrame) -> DataFrame:
    """Add a spending_tier column based on the transaction amount.

    Tiers:
        - "micro"  if amount < 10
        - "small"  if 10 <= amount < 50
        - "medium" if 50 <= amount < 200
        - "large"  if amount >= 200

    For negative amounts (refunds), categorize by absolute value.
    """
    # 1. สร้างคอลัมน์ temp สำหรับเก็บค่า absolute
    df_abs = df.withColumn("abs_amount", F.abs(F.col("amount")))

    # 2. ใช้ when-otherwise เพื่อแบ่งเกรด
    df_result = df_abs.withColumn(
        "spending_tier",
        F.when(F.col("abs_amount") < 10, "micro")
        .when((F.col("abs_amount") >= 10) & (F.col("abs_amount") < 50), "small")
        .when((F.col("abs_amount") >= 50) & (F.col("abs_amount") < 200), "medium")
        .otherwise("large")
    ).drop("abs_amount") # ลบคอลัมน์ชั่วคราวทิ้ง

    return df_result


def enrich_with_lookups(
    df: DataFrame, df_categories: DataFrame, df_merchants: DataFrame
) -> DataFrame:
    """Join transactions with category and merchant lookup tables.

    - Left join on category_id to add: category_name, budget_type
    - Left join on merchant_id to add: merchant_name, merchant_type
    - All transactions must be kept (even if merchant_id has no match)
    """
    # 1. Join กับ Categories
    df_with_categories = df.join(
        df_categories,
        on="category_id",
        how="left"
    )

    # 2. Join กับ Merchants
    df_enriched = df_with_categories.join(
        df_merchants,
        on="merchant_id",
        how="left"
    )

    return df_enriched
