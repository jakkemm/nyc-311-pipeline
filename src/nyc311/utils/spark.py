from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession

from nyc311 import config


def raw_path(day: date) -> Path:
    return config.RAW_DIR / f"{day.isoformat()}.jsonl.gz"

def get_spark_session(app_name: str = "nyc311") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark