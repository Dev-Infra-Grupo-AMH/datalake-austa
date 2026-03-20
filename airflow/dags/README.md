# DAGs - Organização

Estrutura padronizada para DAGs do Airflow.

## Camadas do Data Lake

raw → bronze → silver → silver_context → gold

## Pastas

| Pasta | Responsabilidade |
|-------|-------------------|
| `extraction/` | Extrações batch/micro-batch para camada Raw |
| `orchestration/` | DAGs Cosmos (dbt bronze, silver, silver_context, gold) |
| `delivery/` | Entrega para sistemas externos (ex: FHIR para HAPI FHIR) |
| `streaming/` | DAGs de streaming (Kafka) - Oracle/Tasy no futuro |
| `common/` | Configurações, constantes e default_args compartilhados |
| `tests/` | Testes unitários das DAGs |

## Fluxo

1. **extraction** → Raw (dados brutos, sem transformação)
2. **orchestration** → dbt bronze, silver, silver_context, gold (via Cosmos)
3. **delivery** → silver_context (JSON FHIR) → POST para HAPI FHIR
