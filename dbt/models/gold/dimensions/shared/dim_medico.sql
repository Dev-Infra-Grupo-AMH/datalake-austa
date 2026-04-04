-- Grain: 1 linha = 1 médico (deduplicado por cd_medico)
-- Fonte: silver_context.procedimento via {{ ref('procedimento') }}
-- Camada: Gold | Tipo: SCD1
-- Dimensão conformada — usada por fct_producao_medica (solicitante e executor — role-playing)
-- ⚠️  SCD1 provisório: migrar para SCD2 quando silver_context.medico estiver disponível
{{
  config(
    materialized='table'
    , schema='gold'
    , file_format='iceberg'
    , tags=["gold", "tasy", "medico", "dimensao_conformada"]
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('procedimento') }}
)

-- Consolida médicos solicitantes e executores em um único conjunto
, medicos_brutos AS (
    SELECT cd_medico          AS cd_medico, cd_especialidade FROM source WHERE cd_medico          IS NOT NULL
    UNION
    SELECT cd_medico_executor AS cd_medico, cd_especialidade FROM source WHERE cd_medico_executor IS NOT NULL
    -- Registro sentinela: médico desconhecido / não informado (valor -1 do Tasy)
    UNION ALL
    SELECT -1 AS cd_medico, -1 AS cd_especialidade
)

-- Para cada cd_medico, mantém a especialidade mais recente
, medicos_ranked AS (
    SELECT
        cd_medico
      , cd_especialidade
      , ROW_NUMBER() OVER (PARTITION BY cd_medico ORDER BY cd_especialidade DESC) AS rn
    FROM medicos_brutos
)

, final AS (
    SELECT
          {{ dbt_utils.generate_surrogate_key(['cd_medico']) }}     AS sk_medico
        , cd_medico                                                 AS nk_medico
        , CASE WHEN cd_medico = -1 THEN 'Desconhecido'
               ELSE CAST(cd_especialidade AS STRING)
          END                                                       AS cd_especialidade
        , CASE WHEN cd_medico = -1 THEN TRUE ELSE FALSE END         AS ie_medico_desconhecido
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS _gold_loaded_at
    FROM medicos_ranked
    WHERE rn = 1
)

SELECT * FROM final
