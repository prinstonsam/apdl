from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_impressions(df: DataFrame) -> DataFrame:
    """Deduplicate impressions by id, keeping first occurrence.

    15 duplicate impression ids found in sample data.
    """
    return df.dropDuplicates(["id"])


def clean_clicks(df: DataFrame) -> DataFrame:
    """Fill null revenue with 0.

    Negative revenue kept as-is (refunds/corrections).
    Orphan clicks are dropped downstream by LEFT JOIN.
    """
    return df.withColumn("revenue", F.coalesce(F.col("revenue"), F.lit(0.0)))


def filter_valid_users(df: DataFrame) -> DataFrame:
    """Filter out records with null user_id.

    4 impressions with null user_id cannot be attributed to a user
    and are excluded from per-user spend calculations.
    """
    return df.filter(F.col("user_id").isNotNull())
