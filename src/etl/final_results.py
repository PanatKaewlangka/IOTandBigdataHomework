from src.etl.pipeline import create_spark_session
from src.etl.schemas import transaction_schema, category_schema, merchant_schema
from pyspark.sql import functions as F

def get_final_answers():
    spark = create_spark_session()
    
    # 1. ข้อมูลดิบ (Explore Data)
    df_raw = spark.read.csv("data/transactions.csv", header=True, schema=transaction_schema)
    df_cats = spark.read.csv("data/categories.csv", header=True, schema=category_schema)
    df_merchs = spark.read.csv("data/merchants.csv", header=True, schema=merchant_schema)
    
    print("\n=== [PART 1: DATA EXPLORATION ANSWERS] ===")
    print(f"1. Total transactions: {df_raw.count():,}")
    print(f"2. Unique Members: {df_raw.select('member_id').distinct().count()}")
    print(f"3. Unique Merchants: {df_merchs.count()}")
    print(f"4. Unique Categories: {df_cats.count()}")
    print(f"5. Rows with null amount: {df_raw.filter(F.col('amount').isNull()).count()}")
    date_range = df_raw.select(F.min("date"), F.max("date")).collect()[0]
    print(f"6. Date range: From {date_range[0]} to {date_range[1]}")

    # 2. ข้อมูลหลัง Join (Insights)
    df_enriched = spark.read.parquet("output/analytics/enriched_transactions")
    print("\n=== [PART 4 & 5: ANALYTICS ANSWERS] ===")
    orphan_count = df_enriched.filter(F.col("merchant_name").isNull()).count()
    print(f"1. Transactions with no matching merchant: {orphan_count:,}")
    
    print("\n2. Average transaction amount per year (Trend):")
    spark.read.parquet("output/analytics/avg_amount_by_year").show()
    
    print("\n3. Top spending categories (highest total spending):")
    spark.read.parquet("output/analytics/monthly_by_category").groupBy("category_name") \
        .agg(F.sum("total_amount").alias("grand_total")) \
        .orderBy(F.desc("grand_total")).limit(5).show()

    print("\n4. Spending Tier Distribution:")
    df_enriched.groupBy("spending_tier").count().show()

if __name__ == "__main__":
    get_final_answers()
