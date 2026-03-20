"""
DAG bronze PROC_PACIENTE_VALOR: acionada pelo dataset tasy.PROC_PACIENTE_VALOR.
Quando o stream_tasy_producer emite o dataset (novo Avro em raw/raw-tasy/stream/),
esta DAG executa o modelo dbt bronze_tasy_proc_paciente_valor.
"""
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from common.constants import get_dataset_for_topic
from common.default_args import DEFAULT_ARGS

TOPIC = "tasy.TASY.PROC_PACIENTE_VALOR"
DATASET = get_dataset_for_topic(TOPIC)

DBT_PROJECT_DIR = "/opt/airflow/dbt"
PYTHONPATH = "/opt/airflow/dbt/plugins"


@dag(
    dag_id="bronze_tasy_proc_paciente_valor",
    description="Bronze PROC_PACIENTE_VALOR: Avro raw → Iceberg (acionada por dataset)",
    schedule=[DATASET],
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "dbt", "tasy", "proc_paciente_valor"],
)
def bronze_tasy_proc_paciente_valor_dag():
    run_bronze = BashOperator(
        task_id="run_bronze_tasy_proc_paciente_valor",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"PYTHONPATH={PYTHONPATH} dbt run --select bronze_tasy_proc_paciente_valor"
        ),
    )
    run_bronze


bronze_tasy_proc_paciente_valor_dag()
