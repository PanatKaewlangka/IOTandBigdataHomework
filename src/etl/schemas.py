from pyspark.sql.types import StructType, StructField, StringType, FloatType

# transaction_schema (สำหรับ transactions.csv)
transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("date", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", FloatType(), True),
    StructField("payment_method", StringType(), True),
])

# category_schema (สำหรับ categories.csv)
category_schema = StructType([
    StructField("category_id", StringType(), False),
    StructField("category_name", StringType(), True),
    StructField("budget_type", StringType(), True),
])

# merchant_schema (สำหรับ merchants.csv)
merchant_schema = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_type", StringType(), True),
])
