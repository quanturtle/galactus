from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="supermercados_orchestrator",
    # schedule="0 1 * * *",  # daily 01:00 UTC — paused during rollout
    schedule=None,
    start_date=datetime(2026, 4, 20),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=15),
    },
    tags=["orchestrator", "supermercados"],
) as dag:
    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze",
        trigger_dag_id="a_bronze",
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=True,
    )
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="b_silver",
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=True,
    )
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id="c_gold",
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=True,
    )

    trigger_bronze >> trigger_silver >> trigger_gold
