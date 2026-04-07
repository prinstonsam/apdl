from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.cleaning import filter_valid_users


def join_impressions_clicks(
    impressions: DataFrame, clicks: DataFrame
) -> DataFrame:
    """LEFT JOIN impressions with clicks on impression id.

    Broadcasts clicks (small side) to avoid shuffle skew on impressions.
    Result should be cached by the caller — reused by all three transforms.
    """
    return impressions.join(
        F.broadcast(
            clicks.select(
                F.col("impression_id"),
                F.col("revenue"),
                F.col("id").alias("click_id"),
            )
        ),
        impressions["id"] == clicks["impression_id"],
        how="left",
    )


def calc_metrics(joined: DataFrame) -> DataFrame:
    """Count impressions, clicks, and sum revenue per (app_id, country_code).

    Repartitions by grouping keys before aggregation to distribute
    hot partitions (app_id=1 holds 44% of data).
    """
    return (
        joined.repartition("app_id", "country_code")
        .groupBy("app_id", "country_code")
        .agg(
            F.count("*").alias("impressions"),
            F.count("click_id").alias("clicks"),
            F.coalesce(F.sum("revenue"), F.lit(0)).alias("revenue"),
        )
        .orderBy("app_id", "country_code")
    )


def calc_top_advertisers(joined: DataFrame) -> DataFrame:
    """Return top 5 advertiser_ids by revenue-per-impression (RPI)
    for each (app_id, country_code). Only advertisers with >= 5 impressions qualify.

    Repartitions by full grouping key to spread skewed combinations.
    """
    stats = (
        joined.repartition("app_id", "country_code", "advertiser_id")
        .groupBy("app_id", "country_code", "advertiser_id")
        .agg(
            F.count("*").alias("impressions"),
            F.coalesce(F.sum("revenue"), F.lit(0)).alias("revenue"),
        )
        .filter(F.col("impressions") >= 5)
        .withColumn("rpi", F.col("revenue") / F.col("impressions"))
    )

    w = Window.partitionBy("app_id", "country_code").orderBy(F.col("rpi").desc())

    return (
        stats.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= 5)
        .groupBy("app_id", "country_code")
        .agg(F.collect_list("advertiser_id").alias("recommended_advertiser_ids"))
        .orderBy("app_id", "country_code")
    )


def calc_median_spend(
    impressions: DataFrame, joined: DataFrame
) -> DataFrame:
    """Calculate median spend per user for each country.

    Users without clicks contribute spend=0. Null user_id records are excluded.
    Repartitions by (country_code, user_id) to spread the heaviest user
    (16% of impressions) across partitions.
    """
    return (
        filter_valid_users(joined)
        .repartition("country_code", "user_id")
        .groupBy("country_code", "user_id")
        .agg(F.coalesce(F.sum("revenue"), F.lit(0)).alias("spend"))
        .groupBy("country_code")
        .agg(F.percentile_approx("spend", 0.5).alias("median_spend"))
        .orderBy("country_code")
    )
