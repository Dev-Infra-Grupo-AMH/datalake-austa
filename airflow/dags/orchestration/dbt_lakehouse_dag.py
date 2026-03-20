# """
# DAG Cosmos para orquestração dbt — Lakehouse TASY.
# Executa modelos silver, silver_context e gold via dbt-spark.
# Variáveis Spark (SPARK_THRIFT_*) vêm do ambiente do container.
# """
# from pathlib import Path

# from airflow.decorators import dag
# from airflow.utils.dates import days_ago
# from cosmos.config import ProjectConfig, ProfileConfig, RenderConfig
# from cosmos.constants import LoadMode
# from cosmos.operators.local import DbtTaskGroup

# from common.config import DBT_PROJECT_DIR, DBT_PROFILES_DIR
# from common.default_args import DEFAULT_ARGS

# DBT_PATH = Path(DBT_PROJECT_DIR)
# PROFILES_PATH = Path(DBT_PROFILES_DIR) / "profiles.yml"


# @dag(
#     dag_id="dbt_lakehouse_tasy",
#     default_args=DEFAULT_ARGS,
#     schedule=None,
#     start_date=days_ago(1),
#     tags=["dbt", "cosmos", "lakehouse", "silver", "gold"],
# )
# def dbt_lakehouse_dag():
#     project_config = ProjectConfig(dbt_project_path=str(DBT_PATH))
#     profile_config = ProfileConfig(
#         profile_name="lakehouse_tasy",
#         target_name="dev",
#         profiles_yml_filepath=str(PROFILES_PATH),
#     )
#     render_config = RenderConfig(load_method=LoadMode.CUSTOM)

#     DbtTaskGroup(
#         group_id="dbt_lakehouse",
#         project_config=project_config,
#         profile_config=profile_config,
#         render_config=render_config,
#         operator_args={"install_deps": False},
#     )


# dbt_lakehouse_dag()
