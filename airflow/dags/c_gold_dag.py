from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="c_gold",
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
    tags=["supermercados", "gold"],
) as dag:
    BashOperator(
        task_id="build_gold",
        bash_command="cd /opt/galactus && galactus supermercados build-gold",
    )
