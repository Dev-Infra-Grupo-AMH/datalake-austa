{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='nr_seq_interno',
    incremental_strategy='merge',
    merge_update_columns=[
      'nr_atendimento', 'nr_sequencia', 'cd_setor_atendimento', 'cd_unidade_basica', 'cd_unidade_compl',
      'dh_entrada_unidade', 'dh_atualizacao', 'nm_usuario', 'cd_tipo_acomodacao', 'dh_saida_unidade', 'nr_atend_dia',
      'ds_observacao', 'nm_usuario_original', 'dh_saida_interno', 'ie_passagem_setor', 'nr_acompanhante',
      'ie_calcular_dif_diaria', 'nr_seq_motivo_transf', 'dh_entrada_real', 'nm_usuario_real', 'ie_radiacao',
      'nr_seq_motivo_dif', 'nr_seq_mot_dif_diaria', 'cd_unidade_externa', 'dh_alta_medico_setor', 'cd_motivo_alta_setor',
      'cd_procedencia_setor', 'nr_seq_motivo_int', 'nr_seq_motivo_int_sub', 'nr_seq_motivo_perm', 'dh_atualizacao_nrec',
      'nm_usuario_nrec', 'nr_cirurgia', 'nr_seq_pepo', 'nr_seq_agrupamento', 'qt_tempo_prev', 'nr_seq_unid_ant',
      'dh_saida_temporaria', 'dh_retorno_saida_temporaria', 'id_leito_temp_cross', 'cd_departamento', 'cd_procedencia_atend',
      'nr_seq_classif_esp', 'ie_anzics_generated', 'cd_evolucao', 'dh_est_return',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms', '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id',
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=['silver', 'tasy', 'atend_paciente_unidade'],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_atend_paciente_unidade', 'nr_seq_interno', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_atend_paciente_unidade') }} b
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
          PARTITION BY b.nr_seq_interno
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
      d.nr_seq_interno AS nr_seq_interno
    , {{ fill_null_bigint('d.nr_atendimento', -1) }} AS nr_atendimento
    , {{ fill_null_bigint('d.nr_sequencia', -1) }} AS nr_sequencia
    , {{ fill_null_bigint('d.cd_setor_atendimento', -1) }} AS cd_setor_atendimento
    , {{ fill_null_bigint('d.cd_unidade_basica', -1) }} AS cd_unidade_basica
    , {{ fill_null_bigint('d.cd_unidade_compl', -1) }} AS cd_unidade_compl
    , {{ standardize_timestamp('d.dh_entrada_unidade') }} AS dh_entrada_unidade
    , {{ standardize_timestamp('d.dh_atualizacao') }} AS dh_atualizacao
    , {{ standardize_text_initcap('d.nm_usuario', "'indefinido'") }} AS nm_usuario
    , {{ fill_null_bigint('d.cd_tipo_acomodacao', -1) }} AS cd_tipo_acomodacao
    , {{ standardize_timestamp('d.dh_saida_unidade') }} AS dh_saida_unidade
    , {{ fill_null_bigint('d.nr_atend_dia', -1) }} AS nr_atend_dia
    , {{ standardize_text_initcap('d.ds_observacao', "'indefinido'") }} AS ds_observacao
    , {{ standardize_text_initcap('d.nm_usuario_original', "'indefinido'") }} AS nm_usuario_original
    , {{ standardize_timestamp('d.dh_saida_interno') }} AS dh_saida_interno
    , {{ standardize_enum('d.ie_passagem_setor', "'i'") }} AS ie_passagem_setor
    , {{ fill_null_bigint('d.nr_acompanhante', -1) }} AS nr_acompanhante
    , {{ standardize_enum('d.ie_calcular_dif_diaria', "'i'") }} AS ie_calcular_dif_diaria
    , {{ fill_null_bigint('d.nr_seq_motivo_transf', -1) }} AS nr_seq_motivo_transf
    , {{ standardize_timestamp('d.dh_entrada_real') }} AS dh_entrada_real
    , {{ standardize_text_initcap('d.nm_usuario_real', "'indefinido'") }} AS nm_usuario_real
    , {{ standardize_enum('d.ie_radiacao', "'i'") }} AS ie_radiacao
    , {{ fill_null_bigint('d.nr_seq_motivo_dif', -1) }} AS nr_seq_motivo_dif
    , {{ fill_null_bigint('d.nr_seq_mot_dif_diaria', -1) }} AS nr_seq_mot_dif_diaria
    , {{ fill_null_bigint('d.cd_unidade_externa', -1) }} AS cd_unidade_externa
    , {{ standardize_timestamp('d.dh_alta_medico_setor') }} AS dh_alta_medico_setor
    , {{ fill_null_bigint('d.cd_motivo_alta_setor', -1) }} AS cd_motivo_alta_setor
    , {{ fill_null_bigint('d.cd_procedencia_setor', -1) }} AS cd_procedencia_setor
    , {{ fill_null_bigint('d.nr_seq_motivo_int', -1) }} AS nr_seq_motivo_int
    , {{ fill_null_bigint('d.nr_seq_motivo_int_sub', -1) }} AS nr_seq_motivo_int_sub
    , {{ fill_null_bigint('d.nr_seq_motivo_perm', -1) }} AS nr_seq_motivo_perm
    , {{ standardize_timestamp('d.dh_atualizacao_nrec') }} AS dh_atualizacao_nrec
    , {{ standardize_text_initcap('d.nm_usuario_nrec', "'indefinido'") }} AS nm_usuario_nrec
    , {{ fill_null_bigint('d.nr_cirurgia', -1) }} AS nr_cirurgia
    , {{ fill_null_bigint('d.nr_seq_pepo', -1) }} AS nr_seq_pepo
    , {{ fill_null_bigint('d.nr_seq_agrupamento', -1) }} AS nr_seq_agrupamento
    , {{ normalize_decimal('d.qt_tempo_prev', 2, -1) }} AS qt_tempo_prev
    , {{ fill_null_bigint('d.nr_seq_unid_ant', -1) }} AS nr_seq_unid_ant
    , {{ standardize_timestamp('d.dh_saida_temporaria') }} AS dh_saida_temporaria
    , {{ standardize_timestamp('d.dh_retorno_saida_temporaria') }} AS dh_retorno_saida_temporaria
    , {{ fill_null_string('d.id_leito_temp_cross', "'ind'") }} AS id_leito_temp_cross
    , {{ fill_null_bigint('d.cd_departamento', -1) }} AS cd_departamento
    , {{ fill_null_bigint('d.cd_procedencia_atend', -1) }} AS cd_procedencia_atend
    , {{ fill_null_bigint('d.nr_seq_classif_esp', -1) }} AS nr_seq_classif_esp
    , {{ standardize_enum('d.ie_anzics_generated', "'i'") }} AS ie_anzics_generated
    , {{ fill_null_bigint('d.cd_evolucao', -1) }} AS cd_evolucao
    , {{ standardize_timestamp('d.dh_est_return') }} AS dh_est_return
    {{ silver_audit_columns() }}
  FROM latest_by_pk d
)
, final AS (
  SELECT *
  FROM shaped
  WHERE NOT _is_deleted
)
SELECT * FROM final
