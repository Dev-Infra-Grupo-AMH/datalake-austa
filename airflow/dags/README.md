# DAGs - Organização

Estrutura padronizada para DAGs do Airflow.

## Camadas do Data Lake

raw → bronze → silver → silver_context → gold

## Pastas

| Pasta | Responsabilidade |
|-------|------------------|
| `extraction/` | Extrações batch/micro-batch para camada Raw |
| `orchestration/` | Cosmos + dbt (bronze por tópico, camadas `*_all`, orquestradores stream/batch) |
| `delivery/` | Entrega para sistemas externos (ex.: FHIR para HAPI FHIR) |
| `streaming/` | Datasets / SQS (ex.: `stream_tasy_producer`) |
| `observability/` | Smoke tests dbt, alertas (e-mail se SMTP configurado) |
| `common/` | Config, `cosmos_dbt`, constantes, `default_args` |
| `tests/` | Testes unitários das DAGs |

**Convenção de tags:** [DAG_TAGS.md](DAG_TAGS.md)

## Dependência Cosmos

Instalar na imagem Airflow (não vai pelo rsync de DAGs):

```text
pip install -r airflow/requirements-cosmos.txt
```

## Orquestração dbt (principal)

| DAG / arquivo | Papel |
|---------------|--------|
| `orchestration/bronze/*_dag.py` | Bronze por entidade, `schedule=[Dataset]`, **Cosmos** `DbtTaskGroup` |
| `bronze_dbt_task_group_all.py` | `path:models/bronze` — lote; só batch / manual; **pausada** por padrão |
| `silver_dbt_task_group_all.py` | `path:models/silver` |
| `silver_context_dbt_task_group_all.py` | `path:models/silver_context` |
| `gold_dbt_task_group_all.py` | Scaffold **comentado** até existir gold no dbt |
| `master_dbt_orchestrator_stream.py` | Cron **30 min**: triggers silver → silver_context (gold comentado) |
| `master_dbt_orchestrator_batch.py` | Manual: opcional `dbt run`+vars → triggers bronze_all → silver → context (gold comentado) |

Pool recomendado na UI Airflow: **`spark_dbt`** (slots conforme capacidade Kyuubi).

Variáveis úteis: `DBT_PROJECT_DIR`, `DBT_PROFILES_DIR`, `DBT_TARGET` (target do `profiles.yml`).

## Fluxo

1. **streaming** → emite **Dataset** por tópico quando há evento no S3 raw.
2. **orchestration/bronze/** → Cosmos roda `bronze_tasy_*` por dataset.
3. **master_dbt_orchestrator_stream** → silver → silver_context (gold quando ativado).
4. **master_dbt_orchestrator_batch** → opcional CLI com vars → bronze all → silver → …
5. **delivery** → consumo downstream (ex.: FHIR).
