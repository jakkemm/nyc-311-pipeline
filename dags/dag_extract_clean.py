from datetime import datetime, timedelta

from airflow.decorators import dag, task

from nyc311 import db
from nyc311.extract import socrata_client as sc

LOOKBACK_DAYS = 7
DB_TASK = {"retries": 3, "execution_timeout": timedelta(seconds=60)}
SPARK_TASK = {
    "retries": 0,
    "execution_timeout": timedelta(minutes=10),
    "max_active_tis_per_dag": 1,
}
ARGS = {"owner": "jakkemm", "retry_delay": timedelta(seconds=30)}


def days_to_refresh(nyc_map, db_map):
    return sorted(
        d
        for d, nyc_latest in nyc_map.items()
        if d not in db_map or nyc_latest > db_map[d]
    )


@dag(
    dag_id="nyc311_daily_etl",
    default_args=ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="30 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["nyc311"],
)
def nyc311_daily_etl():
    @task(**DB_TASK)
    def create_table():
        db.create_log_table()

    @task(**DB_TASK)
    def dates_to_refresh(logical_date=None):
        day = logical_date.date()

        nyc_map = sc.fetch_latest_per(day, LOOKBACK_DAYS)
        db_map = db.get_latest_per_day()

        refresh_days = days_to_refresh(nyc_map, db_map)
        print(f"Found {len(refresh_days)} dates to refresh: {refresh_days}")
        return refresh_days

    @task(**DB_TASK)
    def fetch_raw_data(day):
        last_dt = sc.fetch_day(day)
        return (day, last_dt)

    @task.bash(**SPARK_TASK)
    def clean_data(day):
        return f"python /opt/airflow/spark_jobs/run_clean.py {day.date().isoformat()}"

    @task(**DB_TASK)
    def update_db_log(update_rows, *, run_id):
        db.insert_log_rows(run_id, update_rows)

    table = create_table()
    days = dates_to_refresh()

    table >> days

    fetched = fetch_raw_data.expand(day=days)
    cleaned = clean_data.expand(day=days)

    fetched >> cleaned
    update_db_log(fetched) << cleaned


run_day_pipeline = nyc311_daily_etl()
