from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def filter_valid_transactions(df: DataFrame) -> DataFrame:
    """Filter out invalid transactions.

    Rules:
        - Remove rows where amount is null or empty
        - Remove rows where date is outside 2016-01-01 to 2025-12-31
        - Keep negative amounts (these are refunds)
    """
    # 1. กรอง amount ที่เป็น null
    df_filtered = df.filter(F.col("amount").isNotNull())

    # 2. กรองช่วงวันที่ (2016-01-01 ถึง 2025-12-31)
    df_filtered = df_filtered.filter(
        F.col("date").between("2016-01-01", "2025-12-31")
    )

    return df_filtered
