{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.PROC_PACIENTE_VALOR/" %}
{% set cdc_lookback_hours = var('cdc_lookback_hours', 2) %}
{% set cdc_reprocess_hours = var('cdc_reprocess_hours', 0) %}
{% set proc_paciente_valor_business_cols = [
  'ie_tipo_valor',
  'dt_atualizacao',
  'nm_usuario',
  'vl_procedimento',
  'vl_medico',
  'vl_anestesista',
  'vl_materiais',
  'vl_auxiliares',
  'vl_custo_operacional',
  'cd_convenio',
  'cd_categoria',
  'pr_valor',
  'nr_seq_trans_fin',
  'nr_lote_contabil',
  'nr_seq_desconto',
  'qt_pontos',
  'nr_codigo_controle',
  'nr_seq_partic'
] %}

WITH target_watermark AS (
  {% if is_incremental() %}
    SELECT CAST(COALESCE(MAX(_cdc_ts_ms), 0) AS BIGINT) AS max_ts_ms
    FROM {{ this }}
  {% else %}
    SELECT CAST(0 AS BIGINT) AS max_ts_ms
  {% endif %}
)
, params AS (
  SELECT
    CASE
      WHEN {{ cdc_reprocess_hours }} > 0 THEN
        (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - ({{ cdc_reprocess_hours }} * 3600)) * 1000
      ELSE
        GREATEST(
          0
          , (SELECT max_ts_ms FROM target_watermark) - ({{ cdc_lookback_hours }} * 3600 * 1000)
        )
    end  AS wm_start_ms
)
, raw_incremental AS (
  SELECT *
  FROM avro.`{{ raw_path }}`
  WHERE CAST(COALESCE(__ts_ms, 0) AS BIGINT) >= (SELECT wm_start_ms FROM params)
)
SELECT
    nr_seq_procedimento
  , nr_sequencia
  , ie_tipo_valor
  , dt_atualizacao as dh_atualizacao
  , nm_usuario
  , vl_procedimento
  , vl_medico
  , vl_anestesista
  , vl_materiais
  , vl_auxiliares
  , vl_custo_operacional
  , cd_convenio
  , cd_categoria
  , pr_valor
  , nr_seq_trans_fin
  , nr_lote_contabil
  , nr_seq_desconto
  , qt_pontos
  , nr_codigo_controle
  , nr_seq_partic
  {{ bronze_audit_columns(raw_path, proc_paciente_valor_business_cols) }}
  , __source_txid
FROM raw_incremental
