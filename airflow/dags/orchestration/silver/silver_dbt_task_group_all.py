"""
Silver — todos os modelos `path:models/silver`.

Disparada por `master_dbt_orchestrator_stream`, `master_dbt_orchestrator_batch` ou manualmente.
"""
from airflow.decorators import dag
from airflow.utils.dates import days_ago

from common.cosmos_dbt import layer_dbt_task_group
from common.default_args import DEFAULT_ARGS


@dag(
    dag_id="silver_dbt_task_group_all",
    description="Silver (camada): path models/silver",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    tags=["silver", "dbt", "cosmos", "tasy", "layer_all", "lakehouse"],
)
def silver_dbt_task_group_all_dag():
    layer_dbt_task_group("dbt_silver_all", ["path:models/silver"])


silver_dbt_task_group_all_dag()
