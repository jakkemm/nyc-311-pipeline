from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from nyc311.extract.socrata_client import fetch_day, is_day_present

START_DATE = datetime(2026, 1, 1)
ARGS = {
    "owner": "me", 
    "retries": 3, 
    "retry_delay": timedelta(minutes=1)
}


@dag(
    dag_id="nyc311_daily_etl",
    default_args=ARGS,
    start_date=START_DATE,
    schedule="30 3 * * *",
    catchup=False,
    max_active_runs=1,
)
def nyc311_daily_etl():
    @task.sensor(poke_interval=60, timeout=600, mode="reschedule")
    def check_availability() -> bool:
        day = date.fromisoformat(get_current_context()["ds"])
        return is_day_present(day)

    @task(retries=3, retry_delay=timedelta(seconds=30), execution_timeout=timedelta(seconds=120))
    def fetch_raw_data():
        day = date.fromisoformat(get_current_context()["ds"])
        fetch_day(day)

    @task.bash(retries=0, execution_timeout=timedelta(seconds=120))
    def clean_data() -> str:
        return "python /opt/airflow/spark_jobs/run_clean.py {{ ds }}"

    sensor = check_availability()
    raw = fetch_raw_data()
    clean = clean_data()

    sensor >> raw >> clean


run_day_pipeline = nyc311_daily_etl()
