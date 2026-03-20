"""
Constants for DAGs: schemas, buckets, layer names, table prefixes.
Data lake layers: raw, bronze, silver, silver_context, gold.
"""
from airflow.datasets import Dataset

# Schemas (Glue / Spark)
RAW_SCHEMA = "raw"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
SILVER_CONTEXT_SCHEMA = "silver_context"
GOLD_SCHEMA = "gold"

# Paths S3
RAW_TASY_STREAM_PREFIX = "raw/raw_tasy/stream/"
RAW_TASY_BATCH_PREFIX = "raw/raw_tasy/batch/"
RAW_TASY_STREAM_TOPIC_PREFIX = "raw/raw-tasy/stream/"
RAW_TASY_STREAM_FILE_EXTENSION = ".avro"

# Bucket
DATA_LAKE_BUCKET = "austa-lakehouse-prod-data-lake-169446931765"

# Airflow Datasets
RAW_TASY_STREAM_DATASET = Dataset("dataset://raw_tasy_stream")
RAW_TASY_BATCH_DATASET = Dataset("dataset://raw_tasy_batch")

# Mapeamento tópico → dataset
TOPIC_DATASET_MAPPING = {
    "tasy.TASY.ATENDIMENTO_PACIENTE": "tasy.ATENDIMENTO_PACIENTE",
    "tasy.TASY.PROC_PACIENTE_VALOR": "tasy.PROC_PACIENTE_VALOR",
    "tasy.TASY.ATEND_PACIENTE_UNIDADE": "tasy.ATEND_PACIENTE_UNIDADE",
    "tasy.TASY.CONTA_PACIENTE": "tasy.CONTA_PACIENTE",
    "tasy.TASY.PROC_PACIENTE_CONVENIO": "tasy.PROC_PACIENTE_CONVENIO",
    "tasy.TASY.PROCEDIMENTO_PACIENTE": "tasy.PROCEDIMENTO_PACIENTE",
}


def get_dataset_for_topic(topic: str) -> Dataset:
    """Retorna o Dataset Airflow para o tópico dado."""
    dataset_name = TOPIC_DATASET_MAPPING.get(topic)
    if not dataset_name:
        raise ValueError(f"Tópico '{topic}' não está no TOPIC_DATASET_MAPPING")
    path_prefix = f"{RAW_TASY_STREAM_TOPIC_PREFIX}{topic}/"
    return Dataset(f"s3://{DATA_LAKE_BUCKET}/{path_prefix}")
