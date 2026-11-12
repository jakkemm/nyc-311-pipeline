"""
Usage:
    python /opt/airflow/spark_jobs/run_clean.py 2024-01-01
"""
import argparse
from datetime import date

from nyc311.transform.clean import run
from nyc311.utils.spark import get_spark_session


def main():
    parser = argparse.ArgumentParser(description="Clean one day of raw 311 data")
    parser.add_argument("day", type=date.fromisoformat, help="YYYY-MM-DD")
    args = parser.parse_args()

    spark = get_spark_session("nyc311-clean")
    try:
        run(spark, args.day)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()