"""
Bronze — todos os modelos `path:models/bronze` (lote / backfill).

Acionada por `master_dbt_orchestrator_batch` ou manualmente. Não entra no stream orchestrator.
"""
from airflow.decorators import dag
from airflow.utils.dates import days_ago

from common.cosmos_dbt import layer_dbt_task_group
from common.default_args import DEFAULT_ARGS


@dag(
    dag_id="bronze_dbt_task_group_all",
    description="Bronze (lote): path models/bronze — backfill / reprocessamento",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "dbt", "cosmos", "tasy", "layer_all", "lakehouse", "batch"],
)
def bronze_dbt_task_group_all_dag():
    layer_dbt_task_group("dbt_bronze_all", ["path:models/bronze"])


bronze_dbt_task_group_all_dag()
