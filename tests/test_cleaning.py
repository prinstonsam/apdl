import json
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

from src.cleaning import clean_clicks, clean_impressions, filter_valid_users


def read_json(spark: SparkSession, records: list[dict]) -> "DataFrame":
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(records, f)
        path = f.name
    return spark.read.option("multiLine", True).json(path)


def test_clean_impressions_deduplicates(spark: SparkSession) -> None:
    df = read_json(spark, [
        {"id": "id1", "user_id": "u1", "app_id": 1, "country_code": "US", "advertiser_id": 10},
        {"id": "id1", "user_id": "u1", "app_id": 1, "country_code": "US", "advertiser_id": 10},
        {"id": "id2", "user_id": "u2", "app_id": 1, "country_code": "US", "advertiser_id": 20},
    ])
    result = clean_impressions(df)
    assert result.count() == 2


def test_clean_clicks_fills_null_revenue(spark: SparkSession) -> None:
    df = read_json(spark, [
        {"id": "c1", "impression_id": "imp1", "revenue": 1.5},
        {"id": "c2", "impression_id": "imp2", "revenue": None},
        {"id": "c3", "impression_id": "imp3", "revenue": -0.5},
    ])
    result = clean_clicks(df)
    revenues = {r["id"]: r["revenue"] for r in result.collect()}

    assert revenues["c1"] == 1.5
    assert revenues["c2"] == 0.0
    assert revenues["c3"] == -0.5


def test_filter_valid_users_removes_null_user_id(spark: SparkSession) -> None:
    df = read_json(spark, [
        {"id": "id1", "user_id": "u1", "country_code": "US"},
        {"id": "id2", "user_id": None, "country_code": "US"},
        {"id": "id3", "user_id": "u3", "country_code": "CA"},
    ])
    result = filter_valid_users(df)
    assert result.count() == 2
    assert {r["id"] for r in result.collect()} == {"id1", "id3"}
