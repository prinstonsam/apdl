from pyspark.sql import SparkSession

from src.config import AppConfig


def create_spark(config: AppConfig) -> SparkSession:
    """Create SparkSession from application config."""
    return (
        SparkSession.builder
        .appName(config.spark.app_name)
        .master(config.spark.master)
        .getOrCreate()
    )
