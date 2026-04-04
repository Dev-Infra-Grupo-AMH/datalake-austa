-- Grain: 1 linha = 1 código de procedimento (deduplicado por cd_procedimento)
-- Fonte: silver_context.procedimento via {{ ref('procedimento') }}
-- Camada: Gold | Tipo: SCD1 (códigos de procedimento são estáveis)
-- Dimensão conformada — compartilhada entre fatos assistenciais e financeiros
{{
  config(
    materialized='table'
    , schema='gold'
    , file_format='iceberg'
    , tags=["gold", "tasy", "procedimento", "dimensao_conformada"]
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('procedimento') }}
    WHERE cd_procedimento <> -1
)

-- Pega o estado mais recente de cada procedimento via _context_processed_at
, proc_ranked AS (
    SELECT
        cd_procedimento
      , ds_procedimento
      , cd_grupo
      , cd_tabela_servico
      , cd_tabela_custo
      , ROW_NUMBER() OVER (
            PARTITION BY cd_procedimento
            ORDER BY _context_processed_at DESC
        ) AS rn
    FROM source
)

, final AS (
    SELECT
          {{ dbt_utils.generate_surrogate_key(['cd_procedimento']) }}  AS sk_procedimento
        , cd_procedimento                                              AS nk_procedimento
        , ds_procedimento
        , cd_grupo
        , cd_tabela_servico
        , cd_tabela_custo
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS _gold_loaded_at
    FROM proc_ranked
    WHERE rn = 1
)

SELECT * FROM final
