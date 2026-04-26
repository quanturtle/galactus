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
    dag_id="b_silver",
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
    tags=["scraper", "supermercados", "silver"],
) as dag:
    download_tasks = []
    for source in SUPERMERCADOS_SOURCES:
        transform = BashOperator(
            task_id=f"transform_{source}",
            bash_command=(
                f"cd /opt/galactus && galactus supermercados transform --source {source}"
            ),
        )
        download_images = BashOperator(
            task_id=f"download_images_{source}",
            bash_command=(
                f"cd /opt/galactus && galactus supermercados download-images --source {source}"
            ),
        )
        transform >> download_images
        download_tasks.append(download_images)

    standardize_sku = BashOperator(
        task_id="standardize_sku",
        bash_command="cd /opt/galactus && galactus supermercados standardize --step sku",
    )
    standardize_name = BashOperator(
        task_id="standardize_name",
        bash_command="cd /opt/galactus && galactus supermercados standardize --step name",
    )

    download_tasks >> standardize_sku >> standardize_name
