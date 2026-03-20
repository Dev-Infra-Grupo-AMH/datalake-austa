"""
DAG bronze PROCEDIMENTO_PACIENTE: acionada pelo dataset tasy.PROCEDIMENTO_PACIENTE.
Quando o stream_tasy_producer emite o dataset (novo Avro em raw/raw-tasy/stream/),
esta DAG executa o modelo dbt bronze_tasy_procedimento_paciente.
"""
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from common.constants import get_dataset_for_topic
from common.default_args import DEFAULT_ARGS

TOPIC = "tasy.TASY.PROCEDIMENTO_PACIENTE"
DATASET = get_dataset_for_topic(TOPIC)

DBT_PROJECT_DIR = "/opt/airflow/dbt"
PYTHONPATH = "/opt/airflow/dbt/plugins"


@dag(
    dag_id="bronze_tasy_procedimento_paciente",
    description="Bronze PROCEDIMENTO_PACIENTE: Avro raw → Iceberg (acionada por dataset)",
    schedule=[DATASET],
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "dbt", "tasy", "procedimento_paciente"],
)
def bronze_tasy_procedimento_paciente_dag():
    run_bronze = BashOperator(
        task_id="run_bronze_tasy_procedimento_paciente",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"PYTHONPATH={PYTHONPATH} dbt run --select bronze_tasy_procedimento_paciente"
        ),
    )
    run_bronze


bronze_tasy_procedimento_paciente_dag()
