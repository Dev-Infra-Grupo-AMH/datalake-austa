# datalake-austa

Repositório de **orquestração** (Airflow) e **transformações** (dbt) do lakehouse.

| Área | Pasta | Documentação |
|------|--------|--------------|
| DAGs do Airflow | [`airflow/dags/`](airflow/dags/) | DAGs Python em `dags/` |
| Projeto dbt | [`dbt/`](dbt/) | **[dbt/README.md](dbt/README.md)** — como rodar o dbt na sua máquina local |

## Pull requests

Cada PR abre com um [template](.github/pull_request_template.md) que pergunta se a alteração é **feature** ou **bugfix** (organização prévia; ainda sem Gitflow completo). Convenções opcionais: [docs/git-fluxo-previo.md](docs/git-fluxo-previo.md).

## Deploy na EC2

Push na branch **`main`** dispara o pipeline GitHub Actions que sincroniza `airflow/dags/` e `dbt/` para `/opt/airflow/` no servidor.

## Requisitos locais para desenvolvimento

- **dbt:** Python 3.10+, venv e credenciais Spark Thrift — ver [dbt/README.md](dbt/README.md).
- **Airflow:** conforme ambiente do time (Docker/Astro, etc.).

---

*Organização: [Dev-Infra-Grupo-AMH](https://github.com/Dev-Infra-Grupo-AMH)*
