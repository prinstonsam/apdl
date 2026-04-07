import json
import tempfile

from pyspark.sql import DataFrame, SparkSession

from src.transforms import (
    calc_metrics,
    calc_median_spend,
    calc_top_advertisers,
    join_impressions_clicks,
)


def read_json(spark: SparkSession, records: list[dict]) -> DataFrame:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(records, f)
        path = f.name
    return spark.read.option("multiLine", True).json(path)


def make_impressions(spark: SparkSession) -> DataFrame:
    return read_json(spark, [
        {"id": "imp1", "user_id": "u1", "app_id": 1, "country_code": "US", "advertiser_id": 10},
        {"id": "imp2", "user_id": "u1", "app_id": 1, "country_code": "US", "advertiser_id": 10},
        {"id": "imp3", "user_id": "u2", "app_id": 1, "country_code": "US", "advertiser_id": 20},
        {"id": "imp4", "user_id": "u2", "app_id": 1, "country_code": "US", "advertiser_id": 20},
        {"id": "imp5", "user_id": "u3", "app_id": 1, "country_code": "US", "advertiser_id": 10},
        {"id": "imp6", "user_id": "u3", "app_id": 1, "country_code": "US", "advertiser_id": 10},
        {"id": "imp7", "user_id": "u1", "app_id": 1, "country_code": "CA", "advertiser_id": 10},
        {"id": "imp8", "user_id": "u4", "app_id": 1, "country_code": "CA", "advertiser_id": 30},
    ])


def make_clicks(spark: SparkSession) -> DataFrame:
    return read_json(spark, [
        {"id": "c1", "impression_id": "imp1", "revenue": 2.0},
        {"id": "c2", "impression_id": "imp2", "revenue": 3.0},
        {"id": "c3", "impression_id": "imp3", "revenue": 1.0},
        {"id": "c4", "impression_id": "imp7", "revenue": 4.0},
        {"id": "c5", "impression_id": "orphan_id", "revenue": 9.0},
    ])


def make_joined(spark: SparkSession) -> DataFrame:
    return join_impressions_clicks(make_impressions(spark), make_clicks(spark))


class TestJoin:
    def test_left_join_keeps_all_impressions(self, spark: SparkSession) -> None:
        assert make_joined(spark).count() == 8

    def test_orphan_clicks_excluded(self, spark: SparkSession) -> None:
        click_ids = [r["click_id"] for r in make_joined(spark).collect() if r["click_id"]]
        assert "c5" not in click_ids

    def test_unmatched_impressions_have_null_click(self, spark: SparkSession) -> None:
        nulls = make_joined(spark).filter("click_id IS NULL").count()
        assert nulls == 4


class TestCalcMetrics:
    def test_counts_and_revenue(self, spark: SparkSession) -> None:
        rows = {r["country_code"]: r for r in calc_metrics(make_joined(spark)).collect()}

        us = rows["US"]
        assert us["impressions"] == 6
        assert us["clicks"] == 3
        assert abs(us["revenue"] - 6.0) < 0.01

        ca = rows["CA"]
        assert ca["impressions"] == 2
        assert ca["clicks"] == 1
        assert abs(ca["revenue"] - 4.0) < 0.01

    def test_zero_revenue_when_no_clicks(self, spark: SparkSession) -> None:
        from pyspark.sql.types import DoubleType, StringType, StructField, StructType
        impressions = read_json(spark, [
            {"id": "imp1", "user_id": "u1", "app_id": 1, "country_code": "DE", "advertiser_id": 10},
        ])
        click_schema = StructType([
            StructField("id", StringType()),
            StructField("impression_id", StringType()),
            StructField("revenue", DoubleType()),
        ])
        clicks = spark.read.schema(click_schema).option("multiLine", True).json(
            spark.sparkContext.parallelize(["[]"]).pipe("cat")
        )
        # Use empty JSON file approach — create a temp file with explicit schema
        import tempfile, json as _json
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            _json.dump([], f)
            path = f.name
        clicks = spark.read.schema(click_schema).option("multiLine", True).json(path)
        joined = join_impressions_clicks(impressions, clicks)
        row = calc_metrics(joined).collect()[0]
        assert row["clicks"] == 0
        assert row["revenue"] == 0


class TestCalcTopAdvertisers:
    def test_min_5_impressions_filter(self, spark: SparkSession) -> None:
        # All advertisers in base test data have < 5 impressions each
        result = calc_top_advertisers(make_joined(spark))
        assert result.count() == 0

    def test_top5_ranking(self, spark: SparkSession) -> None:
        # 6 advertisers × 5 impressions each, revenue = adv_id per click
        imp_records = [
            {"id": f"imp_{adv}_{i}", "user_id": "u1", "app_id": 1,
             "country_code": "US", "advertiser_id": adv}
            for adv in range(1, 7)
            for i in range(5)
        ]
        clk_records = [
            {"id": f"c_{adv}_{i}", "impression_id": f"imp_{adv}_{i}", "revenue": float(adv)}
            for adv in range(1, 7)
            for i in range(5)
        ]
        impressions = read_json(spark, imp_records)
        clicks = read_json(spark, clk_records)
        joined = join_impressions_clicks(impressions, clicks)
        row = calc_top_advertisers(joined).collect()[0]

        assert len(row["recommended_advertiser_ids"]) == 5
        assert 1 not in row["recommended_advertiser_ids"]  # lowest RPI excluded
        assert row["recommended_advertiser_ids"][0] == 6  # highest RPI first


class TestCalcMedianSpend:
    def test_median_calculation(self, spark: SparkSession) -> None:
        impressions = make_impressions(spark)
        joined = make_joined(spark)
        rows = {r["country_code"]: r["median_spend"] for r in calc_median_spend(impressions, joined).collect()}
        # US: u1=5.0, u2=1.0, u3=0.0 → median=1.0
        assert abs(rows["US"] - 1.0) < 0.01

    def test_null_user_id_excluded(self, spark: SparkSession) -> None:
        impressions = read_json(spark, [
            {"id": "imp1", "user_id": "u1", "app_id": 1, "country_code": "US", "advertiser_id": 10},
            {"id": "imp2", "user_id": None, "app_id": 1, "country_code": "US", "advertiser_id": 10},
        ])
        clicks = read_json(spark, [
            {"id": "c1", "impression_id": "imp1", "revenue": 2.0},
            {"id": "c2", "impression_id": "imp2", "revenue": 100.0},
        ])
        joined = join_impressions_clicks(impressions, clicks)
        row = calc_median_spend(impressions, joined).collect()[0]
        assert abs(row["median_spend"] - 2.0) < 0.01

    def test_users_without_clicks_get_zero_spend(self, spark: SparkSession) -> None:
        impressions = read_json(spark, [
            {"id": "imp1", "user_id": "u1", "app_id": 1, "country_code": "US", "advertiser_id": 10},
            {"id": "imp2", "user_id": "u2", "app_id": 1, "country_code": "US", "advertiser_id": 20},
            {"id": "imp3", "user_id": "u3", "app_id": 1, "country_code": "US", "advertiser_id": 10},
        ])
        clicks = read_json(spark, [
            {"id": "c1", "impression_id": "imp1", "revenue": 4.0},
            {"id": "c3", "impression_id": "imp3", "revenue": 4.0},
        ])
        joined = join_impressions_clicks(impressions, clicks)
        row = calc_median_spend(impressions, joined).collect()[0]
        # u1=4.0, u2=0.0, u3=4.0 → median=4.0
        assert abs(row["median_spend"] - 4.0) < 0.01
