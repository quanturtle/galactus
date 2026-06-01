from airflow.providers.standard.operators.bash import BashOperator

from airflow.sdk import DAG

SOURCE_TYPE = "supermercados"
SOURCE_KIND = "html"
SOURCE = "arete"
PROJECT_DIR = "/home/airflow/galactus"

with DAG(
    dag_id=f"{SOURCE}_pipeline",
    tags=[SOURCE_TYPE, SOURCE_KIND, SOURCE],
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
    load = BashOperator(
        task_id="load",
        cwd=PROJECT_DIR,
        bash_command=f"galactus --config configs/{SOURCE}.yaml --stage load",
    )
    extract >> transform >> load
