"""
DAG bronze ATEND_PACIENTE_UNIDADE: acionada pelo dataset tasy.ATEND_PACIENTE_UNIDADE.
Quando o stream_tasy_producer emite o dataset (novo Avro em raw/raw-tasy/stream/),
esta DAG executa o modelo dbt bronze_tasy_atend_paciente_unidade.
"""
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from common.constants import get_dataset_for_topic
from common.default_args import DEFAULT_ARGS

TOPIC = "tasy.TASY.ATEND_PACIENTE_UNIDADE"
DATASET = get_dataset_for_topic(TOPIC)

DBT_PROJECT_DIR = "/opt/airflow/dbt"
PYTHONPATH = "/opt/airflow/dbt/plugins"


@dag(
    dag_id="bronze_tasy_atend_paciente_unidade",
    description="Bronze ATEND_PACIENTE_UNIDADE: Avro raw → Iceberg (acionada por dataset)",
    schedule=[DATASET],
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "dbt", "tasy", "atend_paciente_unidade"],
)
def bronze_tasy_atend_paciente_unidade_dag():
    run_bronze = BashOperator(
        task_id="run_bronze_tasy_atend_paciente_unidade",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"PYTHONPATH={PYTHONPATH} dbt run --select bronze_tasy_atend_paciente_unidade"
        ),
    )
    run_bronze


bronze_tasy_atend_paciente_unidade_dag()
