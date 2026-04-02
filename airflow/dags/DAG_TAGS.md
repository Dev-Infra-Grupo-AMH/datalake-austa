# Convenção de tags — DAGs Airflow (lakehouse Tasy)

Objetivo: filtrar na UI do Airflow por **camada**, **fonte**, **padrão** (evento vs lote vs orquestração) e **entidade** (quando couber).

**Nota:** tags das DAGs Airflow são **independentes** das chaves `tags:` nos arquivos YAML dos modelos dbt.

## Regras gerais

- Ordem sugerida na lista `tags=[...]`: **camada ou papel** → ferramentas (`dbt`, `cosmos`) → domínio (`tasy`, `lakehouse`).
- Use **snake_case**; evite repetir o `dag_id` inteiro como tag.

## Tabela por tipo de DAG

| Tipo | Tags obrigatórias | Opcional |
|------|-------------------|----------|
| Bronze por tópico (`orchestration/bronze/`) | `bronze`, `dbt`, `cosmos`, `tasy`, `dataset` | entidade (ex.: `atend_paciente_unidade`) — alinhada ao sufixo de `bronze_tasy_*` |
| `bronze_dbt_task_group_all` | `bronze`, `dbt`, `cosmos`, `tasy`, `layer_all`, `lakehouse` | `batch` |
| `silver_dbt_task_group_all` | `silver`, `dbt`, `cosmos`, `tasy`, `layer_all`, `lakehouse` | — |
| `silver_context_dbt_task_group_all` | `silver_context`, `dbt`, `cosmos`, `tasy`, `layer_all`, `lakehouse` | — |
| `gold_dbt_task_group_all` | `gold`, `dbt`, `cosmos`, `tasy`, `layer_all`, `lakehouse` | — |
| `master_dbt_orchestrator_stream` | `orchestrator`, `stream`, `lakehouse`, `dbt` | `cosmos` |
| `master_dbt_orchestrator_batch` | `orchestrator`, `batch`, `lakehouse`, `dbt` | `cosmos` |
| Observabilidade | `observability`, `lakehouse` | `dbt` |

## Ativação da camada gold (Airflow)

1. Criar modelos em `dbt/models/gold/*.sql`.
2. Descomentar `orchestration/gold_dbt_task_group_all.py`.
3. Descomentar os blocos `GOLD_LAYER_TODO` em `master_dbt_orchestrator_stream.py` e `master_dbt_orchestrator_batch.py`.
