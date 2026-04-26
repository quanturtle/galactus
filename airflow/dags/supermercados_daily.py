from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

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
    transforms_done = EmptyOperator(task_id="transforms_done")

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
        scrape >> transform >> transforms_done

    # MLX runs on the host; from inside the airflow container it is at
    # host.docker.internal. Override via GALACTUS_MLX_URL Airflow env var.
    _mlx_env = {
        "GALACTUS_MLX_URL": "{{ var.value.get('galactus_mlx_url', 'http://host.docker.internal:8081') }}",
    }

    standardize_sku = BashOperator(
        task_id="standardize_sku",
        bash_command="cd /opt/galactus && galactus supermercados standardize --step sku",
    )
    standardize_name = BashOperator(
        task_id="standardize_name",
        bash_command="cd /opt/galactus && galactus supermercados standardize --step name",
    )
    standardize_llm = BashOperator(
        task_id="standardize_llm",
        bash_command=(
            "cd /opt/galactus && "
            "galactus supermercados standardize --step llm --limit 2000"
        ),
        env=_mlx_env,
        append_env=True,
        # MLX server may be unreachable; downstream gold rebuild should still run.
        retries=0,
        # ~2000 pairs * ~0.5s = ~17 min; leave headroom over the DAG default.
        execution_timeout=timedelta(minutes=30),
    )
    build_gold = BashOperator(
        task_id="build_gold",
        bash_command="cd /opt/galactus && galactus supermercados build-gold",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    transforms_done >> standardize_sku >> standardize_name >> standardize_llm >> build_gold
