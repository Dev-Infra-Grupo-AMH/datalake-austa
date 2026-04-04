# Gold — Regras de Dimensões (Kimball)

## SCD Type 1 — entidades estáveis (sobrescreve ao mudar)

Usar para: unidade, leito, CID, procedimento_tipo, convenio_tipo

- Surrogate key: `dbt_utils.generate_surrogate_key(['{pk_col}'])`
- Natural key: preservada com prefixo `nk_`
- Materialização: `table` (sobrescreve completo)

## SCD Type 2 — entidades que mudam (preserva histórico)

Usar para: paciente, medico, convenio

- Surrogate key: `dbt_utils.generate_surrogate_key(['{pk_col}', '_silver_processed_at'])`
- Colunas obrigatórias:
  - `_valid_from`   — timestamp início da vigência
  - `_valid_to`     — timestamp fim (NULL = registro atual)
  - `_is_current`   — boolean (TRUE = versão vigente)
- Materialização: `incremental` com `unique_key: sk_{dim}`

## Obrigatório em todas as dimensões

- Surrogate key via `dbt_utils.generate_surrogate_key()`
- Natural key com prefixo `nk_`
- Filtro `WHERE _is_deleted = FALSE` (exceto SCD2 que mantém histórico de deleções)
- `_gold_loaded_at` via `FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')`
- Localização: `models/gold/dimensions/shared/` (sempre conformadas)

## Proibido

- Recriar dimensão que já existe em `/dimensions/shared/` — sempre referenciar
- Surrogate key manual (hash ou concatenação) — usar dbt_utils
- Métricas ou cálculos em dimensões — pertencem aos fatos
