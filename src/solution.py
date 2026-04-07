import sys
from pathlib import Path

from src.cleaning import clean_clicks, clean_impressions
from src.config import load_config
from src.io import read_json_files, write_json
from src.spark import create_spark
from src.transforms import (
    calc_metrics,
    calc_median_spend,
    calc_top_advertisers,
    join_impressions_clicks,
)


def main() -> None:
    """Entry point: load config, read → clean → transform → write."""
    config_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("config.yaml")
    config = load_config(config_path)
    spark = create_spark(config)

    raw_impressions = read_json_files(spark, config.impressions)
    raw_clicks = read_json_files(spark, config.clicks)

    impressions = clean_impressions(raw_impressions)
    clicks = clean_clicks(raw_clicks)

    joined = join_impressions_clicks(impressions, clicks).cache()

    metrics = calc_metrics(joined)
    top_adv = calc_top_advertisers(joined)
    median = calc_median_spend(impressions, joined)

    output = config.output_dir
    write_json(metrics, output / "metrics.json")
    write_json(top_adv, output / "recommendations.json")
    write_json(median, output / "median_spend.json")

    joined.unpersist()
    spark.stop()
    print(f"Results written to {output}/")


if __name__ == "__main__":
    main()
