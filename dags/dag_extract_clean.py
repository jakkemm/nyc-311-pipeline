from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from nyc311.extract.socrata_client import fetch_day, fetch_latest_per

LOOKBACK_DAYS = 7
DB_TASK = {"retries": 3, "execution_timeout": timedelta(seconds=60)}
SPARK_TASK = {
    "retries": 0,
    "execution_timeout": timedelta(minutes=10),
    "max_active_tis_per_dag": 1,
}
ARGS = {"owner": "jakkemm", "retry_delay": timedelta(seconds=30)}


def get_db_map():
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    db_days = hook.get_records("""
        SELECT 
            dag_run_date,
            MAX(latest_date)
        FROM raw_fetch_logs
        GROUP BY dag_run_date
        ORDER BY dag_run_date ASC;
    """)
    return {row[0]: row[1] for row in db_days}


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
        hook = PostgresHook(postgres_conn_id="postgres_localhost")
        hook.run("""
            CREATE TABLE IF NOT EXISTS raw_fetch_logs (
                id BIGSERIAL PRIMARY KEY,
                dt TIMESTAMP DEFAULT LOCALTIMESTAMP,
                dag_run_id TEXT,
                dag_run_date TIMESTAMP,
                latest_date TIMESTAMP
            );
        """)

    @task(**DB_TASK)
    def dates_to_refresh(logical_date=None):
        day = logical_date.date()

        nyc_map = fetch_latest_per(day, LOOKBACK_DAYS)
        db_map = get_db_map()

        refresh_days = days_to_refresh(nyc_map, db_map)
        print(f"Found {len(refresh_days)} dates to refresh: {refresh_days}")
        return refresh_days

    @task(**DB_TASK)
    def fetch_raw_data(day):
        last_dt = fetch_day(day)
        return (day, last_dt)

    @task.bash(**SPARK_TASK)
    def clean_data(day):
        return f"python /opt/airflow/spark_jobs/run_clean.py {day.date().isoformat()}"

    @task(**DB_TASK)
    def update_db_log(update_rows, *, run_id):
        hook = PostgresHook(postgres_conn_id="postgres_localhost")
        rows = [(run_id, *update_row) for update_row in update_rows]

        hook.insert_rows(
            table="raw_fetch_logs",
            rows=rows,
            target_fields=["dag_run_id", "dag_run_date", "latest_date"],
        )

    table = create_table()
    days = dates_to_refresh()

    table >> days

    fetched = fetch_raw_data.expand(day=days)
    cleaned = clean_data.expand(day=days)

    fetched >> cleaned
    update_db_log(fetched) << cleaned


run_day_pipeline = nyc311_daily_etl()
