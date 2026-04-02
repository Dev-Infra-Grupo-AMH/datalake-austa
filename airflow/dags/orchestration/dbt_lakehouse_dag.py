# Orquestração dbt (Cosmos) — substituída pelos módulos abaixo (não registrar DAG aqui).
#
# Ver:
#   - common/cosmos_dbt.py
#   - orchestration/bronze/*_dag.py (bronze por tópico)
#   - orchestration/bronze_dbt_task_group_all.py
#   - orchestration/silver_dbt_task_group_all.py
#   - orchestration/silver_context_dbt_task_group_all.py
#   - orchestration/gold_dbt_task_group_all.py (scaffold comentado)
#   - orchestration/master_dbt_orchestrator_stream.py
#   - orchestration/master_dbt_orchestrator_batch.py
#
# Dependência: astronomer-cosmos (ver airflow/requirements-cosmos.txt).
