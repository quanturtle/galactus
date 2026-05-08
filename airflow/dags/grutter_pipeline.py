from airflow.operators.bash import BashOperator

from airflow import DAG

SOURCE_TYPE = "supermercados"
SOURCE = "grutter"
PROJECT_DIR = "/home/airflow/galactus"

with DAG(
    dag_id=f"{SOURCE}_pipeline",
    tags=["pipeline", SOURCE_TYPE, SOURCE],
) as dag:
    extract = BashOperator(
        task_id="extract",
        cwd=PROJECT_DIR,
        bash_command=f"galactus --config configs/{SOURCE}.yaml --stage extract",
    )
    transform = BashOperator(
        task_id="transform",
        cwd=PROJECT_DIR,
        bash_command=f"galactus --config configs/{SOURCE}.yaml --stage transform",
    )
    extract >> transform
