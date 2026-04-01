{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='nr_seq_procedimento',
    incremental_strategy='merge',
    merge_update_columns=[
      'dt_atualizacao', 'nm_usuario', 'cd_procedimento', 'ds_procedimento', 'cd_unidade_medida', 'tx_conversao_qtde',
      'cd_grupo', 'nr_proc_interno',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms', '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id',
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=['silver', 'tasy', 'proc_paciente_convenio'],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_proc_paciente_convenio', 'nr_seq_procedimento', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_proc_paciente_convenio') }} b
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
          PARTITION BY b.nr_seq_procedimento
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
      d.nr_seq_procedimento AS nr_seq_procedimento
    , {{ standardize_date('d.dh_atualizacao') }} AS dt_atualizacao
    , {{ standardize_text_initcap('d.nm_usuario', "'indefinido'") }} AS nm_usuario
    , {{ fill_null_bigint('d.cd_procedimento', -1) }} AS cd_procedimento
    , {{ standardize_text_initcap('d.ds_procedimento', "'indefinido'") }} AS ds_procedimento
    , {{ fill_null_bigint('d.cd_unidade_medida', -1) }} AS cd_unidade_medida
    , {{ normalize_decimal('d.tx_conversao_qtde', 4, -1) }} AS tx_conversao_qtde
    , {{ fill_null_bigint('d.cd_grupo', -1) }} AS cd_grupo
    , {{ fill_null_bigint('d.nr_proc_interno', -1) }} AS nr_proc_interno
    {{ silver_audit_columns() }}
  FROM latest_by_pk d
)
, final AS (
  SELECT *
  FROM shaped
  WHERE NOT _is_deleted
)
SELECT * FROM final
