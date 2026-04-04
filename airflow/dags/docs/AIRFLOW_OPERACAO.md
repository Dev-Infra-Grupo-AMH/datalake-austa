# Airflow — Operação e infraestrutura (lakehouse Austa)

Documento para **quem mantém** o ambiente: pools, filas Celery, workers Docker e onde alterar cada coisa. Índice geral das DAGs: `README.md` na pasta `dags/`.

## 1. Visão geral da arquitetura (EC2)

- **Um host EC2** executa **Docker Compose** com: Postgres (metadados Airflow), Redis (broker Celery), Webserver, Scheduler, Triggerer, e **dois workers Celery**.
- **Spark / Kyuubi** rodam **fora** deste host; o Airflow apenas orquestra **dbt-spark** (cliente + envio de SQL).

```
Scheduler / Webserver / Triggerer
        │
        ▼
    Redis (filas Celery)
        │
        ├── fila `default` + `stream`  → container `airflow-worker`
        └── fila `dbt`                   → container `airflow-worker-dbt` (mais RAM)
```

## 2. Filas Celery e workers

| Container | Comando (resumo) | Filas escutadas | Memória (referência compose) |
|-----------|------------------|-----------------|------------------------------|
| `airflow-worker` | `celery worker -q default,stream` | `default`, `stream` | ~1,4 GiB |
| `airflow-worker-dbt` | `celery worker -q dbt` | `dbt` | ~4 GiB |

**Regra:** qualquer task **pesada de dbt** (Cosmos ou `BashOperator` com `dbt run` / `dbt test`) deve usar **`queue="dbt"`** para rodar no worker com mais memória.

**Onde está no código**

- Cosmos: `common/cosmos_dbt.py` → `dbt_operator_args()` define `pool` e `queue`.
- Smoke dbt: `observability/lakehouse_dbt_tests_smoke_dag.py`.
- Batch opcional CLI: `orchestration/master_dbt_orchestrator_batch.py` → task `dbt_run_with_vars`.

Tasks leves (triggers, Python, sensors, branch) usam a fila **default** (implícita).

## 3. Pools

Pools limitam **quantas tasks com o mesmo pool** podem estar **rodando ao mesmo tempo** no cluster (além da concorrência Celery).

| Pool | Slots (padrão init) | Uso |
|------|----------------------|-----|
| `default_pool` | 128 (padrão Airflow) | Tasks sem `pool` explícito ou explícito `default_pool`. |
| `spark_dbt` | **1** | **Todas** as execuções dbt-spark via Cosmos + bash dbt que definimos (`pool="spark_dbt"`). Evita vários `dbt run` em paralelo no mesmo host (OOM / Kyuubi). |

**Removido / obsoleto**

- `dbt_bronze_pool`: existia no metadado mas **não era referenciada** no código. O `airflow-init` do Compose passa a executar `airflow pools delete dbt_bronze_pool` (ignora erro se já não existir).

**Ajustar slots** (ex.: subir para 2 após aumentar RAM do `airflow-worker-dbt`):

```bash
docker exec airflow-scheduler airflow pools set spark_dbt 2 'dbt-spark/Cosmos'
```

Atualize também o comando em `docker-compose.yaml` (serviço `airflow-init`) para novos ambientes.

## 4. Onde fica cada ficheiro importante

| Ficheiro | Conteúdo relevante |
|----------|-------------------|
| `/opt/airflow/docker-compose.yaml` | Limites de memória, workers, **comando `airflow-init`** (migrate, user, pools). |
| `dags/common/cosmos_dbt.py` | `pool` + `queue` dos operadores Cosmos. |
| `dags/README.md` | Mapa de DAGs e fluxo lakehouse. |

**Repositório Git:** cópia canónica também em **`airflow/docs/AIRFLOW_OPERACAO.md`**. Este ficheiro em **`dags/docs/`** é incluído no rsync de DAGs para o EC2.

## 5. DAGs e consumo de pool / fila (resumo)

| Área | DAGs (exemplos) | Pool / fila |
|------|-----------------|-------------|
| Bronze / silver / silver_context / gold (Cosmos) | `*_dbt_task_group_all`, `bronze_tasy_*` | `spark_dbt` + `dbt` (via `cosmos_dbt`) |
| Smoke dbt | `lakehouse_dbt_tests_smoke` | `spark_dbt` + `dbt` |
| Batch CLI dbt | `master_dbt_orchestrator_batch` → `dbt_run_with_vars` | `spark_dbt` + `dbt` |
| Orquestradores (só triggers) | `master_dbt_orchestrator_stream`, resto do batch | `default` |
| Streaming | `stream_tasy_producer` | `default` |

## 6. Comandos úteis (manutenção)

```bash
docker exec airflow-scheduler airflow pools list
docker exec airflow-scheduler airflow tasks states-for-dag-run <dag_id> <run_id>
```

## 7. Decisões de desenho (histórico curto)

- **`spark_dbt` com 1 slot:** após **SIGKILL (-9)** por falta de memória com vários modelos dbt em paralelo no mesmo worker.
- **Fila `dbt` dedicada:** isola execução pesada no container com **mais RAM** (`airflow-worker-dbt`).

Para escalar: primeiro **aumentar RAM** do EC2 e do `airflow-worker-dbt`, depois **subir slots** do pool com cautela e monitorizar Kyuubi.
