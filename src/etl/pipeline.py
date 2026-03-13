from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from src.etl.schemas import transaction_schema, category_schema, merchant_schema
from src.etl.validations import filter_valid_transactions
from src.etl.transformations import categorize_spending, enrich_with_lookups
import os

def create_spark_session():
    return (
        SparkSession.builder.master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .appName("PersonalAccountingPipeline")
        .getOrCreate()
    )

def run_pipeline():
    spark = create_spark_session()
    
    # --- STEP 1: LOAD DATA ---
    print("Loading CSV files...")
    df_transactions = spark.read.csv("data/transactions.csv", header=True, schema=transaction_schema)
    df_categories = spark.read.csv("data/categories.csv", header=True, schema=category_schema)
    df_merchants = spark.read.csv("data/merchants.csv", header=True, schema=merchant_schema)

    # --- STEP 2: RAW LAYER (Part 3) ---
    print("Saving Raw layer...")
    df_transactions.write.mode("overwrite").parquet("output/raw/transactions")
    df_categories.write.mode("overwrite").parquet("output/raw/categories")
    df_merchants.write.mode("overwrite").parquet("output/raw/merchants")

    # --- STEP 3: STAGED LAYER (Part 3) ---
    print("Saving Staged layer...")
    df_staged_transactions = filter_valid_transactions(df_transactions)
    df_staged_transactions.write.mode("overwrite").parquet("output/staged/transactions")

    # --- STEP 4: ANALYTICS LAYER - ENRICHED (Part 4) ---
    print("Saving Enriched Analytics...")
    # 1. Categorize spending tiers
    df_categorized = categorize_spending(df_staged_transactions)
    
    # 2. Enrich with lookups (Joins)
    df_enriched = enrich_with_lookups(df_categorized, df_categories, df_merchants)
    df_enriched.write.mode("overwrite").parquet("output/analytics/enriched_transactions")

    # --- STEP 5: ANALYTICS & INSIGHTS (Part 5) ---
    print("Generating Summary Tables...")
    
    # เตรียมคอลัมน์ Year และ Month สำหรับการทำ Aggregation
    df_final = df_enriched.withColumn("year", F.year(F.col("date"))) \
                          .withColumn("month", F.month(F.col("date"))) \
                          .withColumn("year_month", F.date_format(F.col("date"), "yyyy-MM"))

    # 1. Monthly spending by category
    monthly_by_category = df_final.groupBy("year_month", "category_name") \
        .agg(F.sum("amount").alias("total_amount")) \
        .orderBy("year_month", "category_name")
    monthly_by_category.write.mode("overwrite").parquet("output/analytics/monthly_by_category")

    # 2. Yearly spending by member
    yearly_by_member = df_final.groupBy("year", "member_id") \
        .agg(F.sum("amount").alias("total_amount")) \
        .orderBy("year", "member_id")
    yearly_by_member.write.mode("overwrite").parquet("output/analytics/yearly_by_member")

    # 3. Top 10 merchants by total revenue per year
    # ใช้ Window function เพื่อหา Top 10
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("year").orderBy(F.desc("total_amount"))
    
    top_merchants = df_final.groupBy("year", "merchant_name") \
        .agg(F.sum("amount").alias("total_amount")) \
        .withColumn("rank", F.rank().over(window_spec)) \
        .filter(F.col("rank") <= 10) \
        .orderBy("year", F.desc("total_amount"))
    top_merchants.write.mode("overwrite").parquet("output/analytics/top_merchants_by_year")

    # 4. Average transaction amount per year
    avg_amount_by_year = df_final.groupBy("year") \
        .agg(F.avg("amount").alias("avg_amount")) \
        .orderBy("year")
    avg_amount_by_year.write.mode("overwrite").parquet("output/analytics/avg_amount_by_year")

    print("Pipeline completed successfully! Check the 'output/' directory.")

if __name__ == "__main__":
    run_pipeline()
