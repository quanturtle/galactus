from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

NOTICIAS_SOURCES = [
    "abc_color",
    "elnacional",
    "hoy",
    "lanacion",
    "latribuna",
    "megacadena",
    "npy",
    "ultimahora",
]

with DAG(
    dag_id="noticias_daily",
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
    tags=["scraper", "noticias"],
) as dag:
    for source in NOTICIAS_SOURCES:
        scrape = BashOperator(
            task_id=f"scrape_{source}",
            bash_command=(
                f"cd /opt/galactus && galactus noticias scrape --source {source}"
            ),
        )
        transform = BashOperator(
            task_id=f"transform_{source}",
            bash_command=(
                f"cd /opt/galactus && galactus noticias transform --source {source}"
            ),
        )
        download_images = BashOperator(
            task_id=f"download_images_{source}",
            bash_command=(
                f"cd /opt/galactus && galactus noticias download-images --source {source}"
            ),
        )
        scrape >> transform >> download_images
