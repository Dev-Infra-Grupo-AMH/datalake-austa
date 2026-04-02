"""
Gold — camada `path:models/gold` (Cosmos).

SCAFFOLD DESATIVADO: descomente o bloco abaixo quando existir pelo menos um modelo em
`dbt/models/gold/*.sql`. Em seguida descomente os trechos GOLD_LAYER_TODO em:
  - master_dbt_orchestrator_stream.py
  - master_dbt_orchestrator_batch.py
"""
# ---------------------------------------------------------------------------
# GOLD — descomente quando a camada gold existir no dbt
# ---------------------------------------------------------------------------
# from airflow.decorators import dag
# from airflow.utils.dates import days_ago
#
# from common.cosmos_dbt import layer_dbt_task_group
# from common.default_args import DEFAULT_ARGS
#
#
# @dag(
#     dag_id="gold_dbt_task_group_all",
#     description="Gold (camada): path models/gold",
#     schedule=None,
#     start_date=days_ago(1),
#     catchup=False,
#     is_paused_upon_creation=False,
#     default_args=DEFAULT_ARGS,
#     tags=["gold", "dbt", "cosmos", "tasy", "layer_all", "lakehouse"],
# )
# def gold_dbt_task_group_all_dag():
#     layer_dbt_task_group("dbt_gold_all", ["path:models/gold"])
#
#
# gold_dbt_task_group_all_dag()
