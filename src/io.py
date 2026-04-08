import json
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def read_json_files(spark: SparkSession, paths: list[Path]) -> DataFrame:
    """Read and union multiple JSON files into a single DataFrame."""
    str_paths = [str(p) for p in paths]
    return reduce(
        DataFrame.unionByName,
        (spark.read.option("multiLine", True).json(p) for p in str_paths),
    )


def write_json(df: DataFrame, path: Path) -> None:
    """Collect DataFrame and write as a formatted JSON array."""
    # The volume of data is small, which is why this approach is used here
    path.parent.mkdir(parents=True, exist_ok=True)
    records = [row.asDict(recursive=True) for row in df.collect()]
    path.write_text(json.dumps(records, indent=2, ensure_ascii=False))

    # Write DataFrame as JSON
    # df.coalesce(1).write.mode("overwrite").json(str(path))