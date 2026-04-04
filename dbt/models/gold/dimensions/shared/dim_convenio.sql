-- Grain: 1 linha = 1 convênio (deduplicado por cd_convenio)
-- Fonte: silver_context.procedimento via {{ ref('procedimento') }}
-- Camada: Gold | Tipo: SCD1
-- Dimensão conformada — compartilhada entre fatos assistenciais e financeiros
-- ⚠️  SCD1 provisório: migrar para SCD2 quando silver_context.convenio estiver disponível
{{
  config(
    materialized='table'
    , schema='gold'
    , file_format='iceberg'
    , tags=["gold", "tasy", "convenio", "dimensao_conformada"]
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('procedimento') }}
    WHERE cd_convenio <> -1
)

, convenio_ranked AS (
    SELECT
        cd_convenio
      , cd_grupo
      , ROW_NUMBER() OVER (
            PARTITION BY cd_convenio
            ORDER BY _context_processed_at DESC
        ) AS rn
    FROM source
)

, final AS (
    SELECT
          {{ dbt_utils.generate_surrogate_key(['cd_convenio']) }}  AS sk_convenio
        , cd_convenio                                             AS nk_convenio
        , cd_grupo
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS _gold_loaded_at
    FROM convenio_ranked
    WHERE rn = 1
)

SELECT * FROM final
