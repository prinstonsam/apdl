from pathlib import Path

import yaml
from pydantic import BaseModel


class SparkConfig(BaseModel):
    app_name: str = "appodeal-challenge"
    master: str = "local[*]"


class AppConfig(BaseModel):
    spark: SparkConfig = SparkConfig()
    impressions: list[Path]
    clicks: list[Path]
    output_dir: Path = Path("output_data")


def load_config(path: Path) -> AppConfig:
    """Load and validate application config from a YAML file."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    return AppConfig.model_validate(raw)
