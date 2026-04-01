# Fluxo de trabalho, usabilidade e dicas — dbt Austa

Guia para **quem está começando**: o que fazer, em que ordem, e como evitar os erros mais comuns. Comandos assumem terminal na pasta `**datalake-austa/dbt`** com o **venv** ativo.

---

## 1. O que você vai fazer no dia a dia


| Situação                   | Ação típica                                                                                                                 |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| Entrar no projeto          | Clonar repo, criar/ativar venv, `pip install -r requirements-dbt.txt`                                                       |
| Configurar primeira vez    | Copiar `.env.example` e `profiles.example.yml` (a partir dos exemplos do time), preencher Thrift/LDAP e retirar o "example" |
| Validar ambiente           | `dbt debug`                                                                                                                 |
| Rodar uma camada           | `dbt run --select models/bronze` (ou `silver`, etc.)                                                                        |
| Rodar um modelo            | `dbt run --select nome_do_modelo`                                                                                           |
| Validar dados / contratos  | `dbt test` ou `dbt test --select nome_do_modelo`                                                                            |
| Ver SQL compilado          | `dbt compile --select nome_do_modelo` e abrir `target/compiled/...`                                                         |
| Bronze com reprocessamento | Ver [RUNBOOK_CDC_BRONZE.md](../RUNBOOK_CDC_BRONZE.md)                                                                       |


---

## 2. Fluxo recomendado para o primeiro “run”

1. **VPN / rede** — garantir acesso ao host Kyuubi/Thrift.
2. **Python 3.10+** e venv na raiz do clone **ou** em `dbt/` (como descrito no [README.md](../README.md)).
3. `**pip install -r requirements-dbt.txt`** na pasta `dbt/`.
4. Arquivos `**dbt/.env`** e `**dbt/profiles.yml**` (nunca commitar).
5. Na pasta `dbt/` (ou qualquer pasta sob o clone, para o auto-detect do `sitecustomize`):
  ```bash
   dbt debug
  ```
6. Se OK, teste um modelo pequeno ou a camada desejada:
  ```bash
   dbt run --select bronze_tasy_atend_paciente_unidade
  ```

---

## 3. Windows (PowerShell) — dicas essenciais

- **Sempre** ative o venv do projeto antes de rodar `dbt`.
- Confirme o executável:
  ```powershell
  (Get-Command dbt).Source
  ```
  Deve apontar para `...\datalake-austa\.venv\Scripts\dbt.exe` (ou o venv onde você instalou).
- Se o PATH apontar para outro Python, use explicitamente:
  ```powershell
  python -m dbt.cli.main debug
  ```

---

## 4. Seleção de modelos (dbt “node selection”)


| Objetivo                       | Comando                                              |
| ------------------------------ | ---------------------------------------------------- |
| Toda a bronze                  | `dbt run --select models/bronze`                     |
| Toda a silver                  | `dbt run --select models/silver`                     |
| Bronze + tudo que depende dela | `dbt run --select +silver_tasy_atendimento_paciente` |
| Um modelo + upstream           | `dbt run --select +nome_modelo`                      |
| Um modelo + downstream         | `dbt run --select nome_modelo+`                      |


Documentação oficial: [dbt node selection](https://docs.getdbt.com/reference/node-selection/syntax).

---

## 5. Variáveis do projeto (`vars`)

Definidas em `dbt_project.yml` (ex.: `cdc_lookback_hours`, `cdc_reprocess_hours`). Sobrescrever na CLI:

```bash
dbt run --select bronze_tasy_atendimento_paciente --vars '{"cdc_reprocess_hours": 24}'
```

---

## 6. Onde está a “verdade” sobre o pipeline


| Pergunta                                       | Documento                                              |
| ---------------------------------------------- | ------------------------------------------------------ |
| Como o hospital encaixa Tasy, S3, Spark e dbt? | [ARCHITECTURE.md](ARCHITECTURE.md)                     |
| O que cada camada promete?                     | [DATA_CONTRACTS.md](DATA_CONTRACTS.md)                 |
| Como criar modelo novo (junior)?               | [../../docs/dbt_camadas.md](../../docs/dbt_camadas.md) |
| Padrão SQL bronze CDC                          | [../../docs/bronze.md](../../docs/bronze.md)           |
| Macros                                         | [MACROS.md](MACROS.md)                                 |
| `.env` / patch Spark                           | [PLUGINS.md](PLUGINS.md)                               |
| Airflow: SQS, datasets, DAGs bronze            | `airflow/dags/README.md` e `ARCHITECTURE.md`           |


---

## 7. Problemas frequentes (resumo)

Sintomas e soluções detalhadas estão no [README.md §8](../README.md#8-problemas-comuns). Atalhos:

- `**Env var required but not provided: 'SPARK_THRIFT_HOST'**` — falta `dbt/.env` ou venv sem `python-dotenv`/plugins.
- `**Could not find profile named 'lakehouse_tasy'**` — `DBT_PROFILES_DIR` errado; rode a partir do clone ou defina a pasta `dbt/` explicitamente.
- `**dbt.adapters.factory` / módulo dbt estranho** — você não está usando o `dbt` do venv; use `python -m dbt.cli.main`.
- **Timeout / connection refused** — rede, VPN, host/porta do Thrift.

---

## 8. Boas práticas de trabalho em equipe

1. **Antes de abrir PR:** `dbt compile` ou `dbt run --select seu_modelo` + `dbt test --select seu_modelo`.
2. **Nomenclatura:** seguir `CLAUDE.md` e [dbt_camadas.md](../../docs/dbt_camadas.md).
3. **Bronze:** não adicionar joins; manter `schema.yml` alinhado ao `SELECT`.
4. **Silver:** sempre `post_hook_delete_source_tombstones` coerente com a bronze e PK corretas.
5. **Documentação:** descrições nos `schema.yml` para modelos e colunas relevantes.

---

## 9. Airflow vs máquina local

- **Produção / EC2:** DAGs bronze disparam `dbt run` no servidor (ver `ARCHITECTURE.md`).
- **Desenvolvimento:** use o mesmo repositório localmente para iterar mais rápido; alinhar branch com o que será deployado (ex.: `main` via GitHub Actions, conforme README).

