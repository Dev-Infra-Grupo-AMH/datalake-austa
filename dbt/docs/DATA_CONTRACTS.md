# Contratos de dados por camada (dbt — Austa)

Resumo do que cada camada **deve** entregar no projeto atual, alinhado a `dbt/models/` e às macros em `dbt/macros/`. Para padrões SQL da bronze CDC, ver também [docs/bronze.md](../../docs/bronze.md).

---

## Bronze (`models/bronze/`)

**Objetivo:** ingerir o stream CDC já landado em **Avro no S3**, com janela incremental por `__ts_ms`, padronização mínima (ex.: `dt_*` → `dh_*` no SELECT) e **metadados de auditoria** — **sem joins** entre entidades e **sem regras analíticas**.

**Implementação atual (repositório):**

- Materialização **incremental** com `incremental_strategy='append'` e **Iceberg**.
- Watermark na própria tabela bronze (`MAX(_cdc_ts_ms)`) + `vars` `cdc_lookback_hours` / `cdc_reprocess_hours`.
- Colunas de auditoria via macro `bronze_audit_columns` (`_is_deleted`, `_cdc_op`, `_cdc_ts_ms`, `_cdc_event_at`, `_cdc_source_table`, `_source_path`, `_row_hash`, `_bronze_loaded_at`).
- **Efeito:** podem existir **várias linhas por chave de negócio** ao longo do tempo; a **deduplicação “estado atual”** é responsabilidade da **silver**.

**Consumidor downstream:** apenas a **silver** (via `ref('bronze_tasy_*')`), não analistas finais.

---

## Silver (`models/silver/`)

**Objetivo:** **uma linha por chave de negócio** (estado atual), com **limpeza e padronização** (macros de data, texto, documentos, numéricos) e **linhagem CDC** preservada para tombstones.

**Implementação atual:**

- `ref()` exclusivamente a modelos **bronze** da mesma entidade.
- Filtro incremental por `_bronze_loaded_at` (novos eventos na bronze desde a última execução silver).
- CTE `latest_by_pk`: `ROW_NUMBER() PARTITION BY <pk> ORDER BY _cdc_ts_ms DESC, __source_txid DESC`.
- Materialização **incremental** + `incremental_strategy='merge'` + `unique_key` = PK de negócio.
- `merge_update_columns` lista colunas atualizáveis; **`_silver_processed_at` não entra no merge** (primeiro processamento imutável).
- `post_hook_delete_source_tombstones(...)`: remove da silver linhas cujo PK ainda existe na bronze com `_is_deleted = true`.
- Macro `silver_audit_columns()` para colunas de auditoria silver.

**Consumidor downstream:** **silver_context** e futura **gold**; analistas podem consultar silver quando precisarem do grão entidade sem joins.

---

## Silver-Context (`models/silver_context/`)

**Objetivo:** **combinar** entidades já deduplicadas na silver (**joins**, enriquecimento) para **casos de uso analíticos** (ex.: atendimento + conta).

**Implementação atual:**

- `ref()` apenas a `silver_tasy_*` (não ler bronze diretamente).
- Em geral `materialized='table'` + Iceberg.
- Macros de padronização reutilizadas quando o SELECT expõe colunas vindas das silver.
- `silver_context_audit_columns()` para `_context_processed_at` e `_dbt_invocation_id`.

**Sem nova deduplicação por PK de fonte** — assume silver já correta.

---

## Gold (`models/gold/`)

**Objetivo (Kimball / BI):** dimensões e fatos com **chaves substitutas**, integridade referencial e testes `relationships` — conforme evolução do projeto.

**Estado atual:** existe `models/gold/schema.yml` com exemplo documentado (`gold_dim_paciente`); **não há arquivos `.sql` gold no repositório neste momento**. Ao adicionar modelos, seguir `CLAUDE.md` (nomes `gold_dim_*`, `gold_fct_*`, `dbt_utils.generate_surrogate_key`, etc.).

---

## Matriz rápida

| Pergunta | Bronze | Silver | Silver-Context | Gold |
|----------|--------|--------|----------------|------|
| Lê Avro/S3 direto? | Sim | Não | Não | Não |
| Usa `ref()` bronze? | Não | Sim | Não | Não (via silver/context) |
| Usa `ref()` silver? | Não | Não | Sim | Depende do desenho |
| Dedup por PK negócio? | Não (append) | Sim | Não | N/A |
| Joins entre entidades? | Não | Não | Sim | Sim (fatos ↔ dims) |

---

## Nomenclatura obrigatória (modelos)

| Camada | Padrão de nome | Pasta |
|--------|----------------|--------|
| Bronze | `bronze_tasy_{entidade}` | `models/bronze/` |
| Silver | `silver_tasy_{entidade}` | `models/silver/` |
| Silver-Context | nome semântico (ex.: `atendimento`) | `models/silver_context/` |
| Gold | `gold_dim_*` / `gold_fct_*` | `models/gold/` |

Lista dos modelos **existentes** hoje: [dbt_camadas.md — inventário](../../docs/dbt_camadas.md#inventário-de-modelos-no-repositório).
