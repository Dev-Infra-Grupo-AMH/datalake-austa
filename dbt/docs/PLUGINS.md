# Plugins e extensões — `dbt/plugins/`

O diretório `dbt/plugins/` é instalado como pacote Python **editável** via `requirements-dbt.txt`:

```text
-e ./plugins
```

Isso coloca `sitecustomize.py` no ambiente do venv. O Python importa `sitecustomize` automaticamente na inicialização, **antes** do CLI do dbt rodar.

---

## Pacote `lakehouse-dbt-spark-patch`

Definido em `plugins/pyproject.toml`:

- **Nome:** `lakehouse-dbt-spark-patch`
- **Dependência declarada:** `python-dotenv>=1.0.0`
- **Módulo:** `sitecustomize` (arquivo `plugins/sitecustomize.py`)

---

## O que `sitecustomize.py` faz

### 1. `DBT_PROFILES_DIR` e arquivo `.env`

- Se **`DBT_PROFILES_DIR`** já estiver definido (ex.: Airflow/CI), carrega apenas `$(DBT_PROFILES_DIR)/.env` se existir, **sem sobrescrever** variáveis já definidas no ambiente.
- Se **não** estiver definido, sobe diretórios a partir do `cwd` (até 12 níveis) procurando `dbt_project.yml` na pasta atual ou em `subdir/dbt/`, define `DBT_PROFILES_DIR` para essa pasta e carrega `dbt/.env` com `override=False`.

**Efeito para desenvolvedores:** ao rodar `dbt` de dentro da árvore do clone, em geral **não** é necessário exportar `DBT_PROFILES_DIR` nem fazer `source .env` manualmente.

### 2. Patch do adaptador **dbt-spark** (Thrift / PyHive)

- Na conexão **Thrift**, injeta `database` em `pyhive.hive.connect` a partir de `creds.schema` (último segmento se vier como `catalog.schema`), alinhado ao **Glue**.
- Inclui workaround para **Iceberg v2**: quando o Spark retorna erro do tipo *"SHOW TABLE EXTENDED is not supported for v2 tables"*, o cursor substitui por estratégia baseada em `SHOW TABLES` para a introspecção que o dbt-spark faz.

Sem esse patch, conexões Thrift + Glue e tabelas Iceberg v2 podem falhar em `dbt run` / `dbt docs` dependendo da versão do cluster.

---

## Airflow (EC2)

Nas DAGs dbt no Airflow (**Cosmos** `DbtTaskGroup`, ver `airflow/dags/common/cosmos_dbt.py`), o `operator_args` define `env` com **`PYTHONPATH`** incluindo `/opt/airflow/dbt/plugins` (ou `{DBT_PROJECT_DIR}/plugins`), espelhando o que antes era passado no `BashOperator` das bronzes.

Assim o mesmo `sitecustomize` (ou módulos em `plugins/`) pode ser carregado no worker, alinhado ao desenvolvimento local quando o processo Python executa o `dbt`.

---

## Verificação rápida

1. Venv ativo com `pip install -r requirements-dbt.txt`.
2. `python -c "import sitecustomize; print('ok')"` executado na pasta `dbt/` não deve falhar.
3. `dbt debug` com `dbt/.env` preenchido deve resolver `SPARK_THRIFT_*` sem export manual (desde que `DBT_PROFILES_DIR` não esteja definido incorretamente).

---

## Referência

- Guia de uso local: [FLUXO_USO_E_DICAS.md](FLUXO_USO_E_DICAS.md)
- README principal: [../README.md](../README.md)
