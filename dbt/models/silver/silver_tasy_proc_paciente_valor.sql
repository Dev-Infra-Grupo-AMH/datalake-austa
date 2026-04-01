{#- Silver: one column per line + macros (dbt/macros/). PK: nr_sequencia. -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='nr_sequencia',
    incremental_strategy='merge',
    merge_update_columns=[
      'nr_seq_procedimento', 'ie_tipo_valor', 'dt_atualizacao', 'nm_usuario', 'vl_procedimento', 'vl_medico', 'vl_anestesista',
      'vl_materiais', 'vl_auxiliares', 'vl_custo_operacional', 'cd_convenio', 'cd_categoria', 'pr_valor', 'nr_seq_trans_fin',
      'nr_lote_contabil', 'nr_seq_desconto', 'qt_pontos', 'nr_codigo_controle', 'nr_seq_partic',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms', '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id',
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=['silver', 'tasy', 'proc_paciente_valor'],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_proc_paciente_valor', 'nr_sequencia', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_proc_paciente_valor') }} b
  {% if is_incremental() %}
  WHERE b._bronze_loaded_at > (
      SELECT COALESCE(MAX(s._silver_processed_at), CAST('1900-01-01' AS TIMESTAMP))
      FROM {{ this }} s
  )
  {% endif %}
)
, latest_by_pk AS (
  SELECT *
  FROM (
    SELECT
        b.*
      , ROW_NUMBER() OVER (
          PARTITION BY b.nr_sequencia
          ORDER BY
              CAST(COALESCE(b._cdc_ts_ms, 0) AS BIGINT) DESC
            , CAST(COALESCE(b.__source_txid, 0) AS BIGINT) DESC
        ) AS _rn
    FROM base b
  ) x
  WHERE _rn = 1
)
, shaped AS (
  SELECT
      {{ fill_null_bigint('d.nr_seq_procedimento', -1) }} AS nr_seq_procedimento
    , d.nr_sequencia AS nr_sequencia
    , {{ standardize_enum('d.ie_tipo_valor', "'i'") }} AS ie_tipo_valor
    , {{ standardize_date('d.dh_atualizacao') }} AS dt_atualizacao
    , {{ standardize_text_initcap('d.nm_usuario', "'indefinido'") }} AS nm_usuario
    , {{ normalize_decimal('d.vl_procedimento', 2, -1) }} AS vl_procedimento
    , {{ normalize_decimal('d.vl_medico', 2, -1) }} AS vl_medico
    , {{ normalize_decimal('d.vl_anestesista', 2, -1) }} AS vl_anestesista
    , {{ normalize_decimal('d.vl_materiais', 2, -1) }} AS vl_materiais
    , {{ normalize_decimal('d.vl_auxiliares', 2, -1) }} AS vl_auxiliares
    , {{ normalize_decimal('d.vl_custo_operacional', 2, -1) }} AS vl_custo_operacional
    , {{ fill_null_bigint('d.cd_convenio', -1) }} AS cd_convenio
    , {{ fill_null_bigint('d.cd_categoria', -1) }} AS cd_categoria
    , {{ normalize_decimal('d.pr_valor', 2, -1) }} AS pr_valor
    , {{ fill_null_bigint('d.nr_seq_trans_fin', -1) }} AS nr_seq_trans_fin
    , {{ fill_null_bigint('d.nr_lote_contabil', -1) }} AS nr_lote_contabil
    , {{ fill_null_bigint('d.nr_seq_desconto', -1) }} AS nr_seq_desconto
    , {{ normalize_decimal('d.qt_pontos', 2, -1) }} AS qt_pontos
    , {{ fill_null_bigint('d.nr_codigo_controle', -1) }} AS nr_codigo_controle
    , {{ fill_null_bigint('d.nr_seq_partic', -1) }} AS nr_seq_partic
    {{ silver_audit_columns() }}
  FROM latest_by_pk d
)
, final AS (
  SELECT *
  FROM shaped
  WHERE NOT _is_deleted
)
SELECT * FROM final
