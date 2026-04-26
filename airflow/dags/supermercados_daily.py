from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

SUPERMERCADOS_SOURCES = [
    "arete",
    "biggie",
    "casarica",
    "grutter",
    "superseis",
]

with DAG(
    dag_id="supermercados_daily",
    # schedule="0 1 * * *",  # daily 01:00 UTC — paused during rollout
    schedule=None,
    start_date=datetime(2026, 4, 20),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=15),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["scraper", "supermercados"],
) as dag:
    for source in SUPERMERCADOS_SOURCES:
        scrape = BashOperator(
            task_id=f"scrape_{source}",
            bash_command=(
                f"cd /opt/galactus && galactus supermercados scrape --source {source}"
            ),
        )
        transform = BashOperator(
            task_id=f"transform_{source}",
            bash_command=(
                f"cd /opt/galactus && galactus supermercados transform --source {source}"
            ),
        )
        scrape >> transform
