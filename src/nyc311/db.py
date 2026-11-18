from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN_ID = "postgres_localhost"


def _hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=PG_CONN_ID)


def create_log_table() -> None:
    hook = _hook()
    hook.run("""
        CREATE TABLE IF NOT EXISTS raw_fetch_logs (
            id BIGSERIAL PRIMARY KEY,
            dt TIMESTAMP DEFAULT LOCALTIMESTAMP,
            dag_run_id TEXT,
            dag_run_date TIMESTAMP,
            latest_date TIMESTAMP
        );
    """)


def get_latest_per_day():
    hook = _hook()
    db_days = hook.get_records("""
        SELECT 
            dag_run_date,
            MAX(latest_date)
        FROM raw_fetch_logs
        GROUP BY dag_run_date
        ORDER BY dag_run_date ASC;
    """)
    return {row[0]: row[1] for row in db_days}


def insert_log_rows(run_id: str, rows: list[tuple]) -> None:
    hook = _hook()
    rows = [(run_id, *row) for row in rows]

    hook.insert_rows(
        table="raw_fetch_logs",
        rows=rows,
        target_fields=["dag_run_id", "dag_run_date", "latest_date"],
    )
