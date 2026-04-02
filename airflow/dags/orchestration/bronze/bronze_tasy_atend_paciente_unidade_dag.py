"""
DAG bronze ATEND_PACIENTE_UNIDADE: acionada pelo dataset tasy.ATEND_PACIENTE_UNIDADE.
Quando o stream_tasy_producer emite o dataset (novo Avro em raw/raw-tasy/stream/),
esta DAG executa o modelo dbt bronze_tasy_atend_paciente_unidade (Cosmos / dbt-spark).
"""
from airflow.decorators import dag
from airflow.utils.dates import days_ago

from common.constants import get_dataset_for_topic
from common.cosmos_dbt import layer_dbt_task_group
from common.default_args import DEFAULT_ARGS

TOPIC = "tasy.TASY.ATEND_PACIENTE_UNIDADE"
DATASET = get_dataset_for_topic(TOPIC)

MODEL = "bronze_tasy_atend_paciente_unidade"


@dag(
    dag_id="bronze_tasy_atend_paciente_unidade",
    description="Bronze ATEND_PACIENTE_UNIDADE: Avro raw → Iceberg (acionada por dataset)",
    schedule=[DATASET],
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "dbt", "cosmos", "tasy", "dataset", "atend_paciente_unidade"],
)
def bronze_tasy_atend_paciente_unidade_dag():
    layer_dbt_task_group("dbt_bronze", [MODEL])


bronze_tasy_atend_paciente_unidade_dag()
