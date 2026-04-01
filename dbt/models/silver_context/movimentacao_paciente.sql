{#-
  Silver-Context: Movimentação Paciente
  Join: atend_paciente_unidade + atendimento_paciente (contexto).
  Grão: 1 linha por passagem em unidade assistencial.
  PK: nr_seq_interno.
  Tratamento de nulos: mesmos macros da camada silver.
-#}
{{
  config(
    materialized='table'
    , schema='silver_context'
    , file_format='iceberg'
    , tags=["silver_context", "tasy", "movimentacao_paciente"]
  )
}}

WITH unidade AS (
  SELECT * FROM {{ ref('silver_tasy_atend_paciente_unidade') }}
)
, atend AS (
  SELECT * FROM {{ ref('silver_tasy_atendimento_paciente') }}
)
SELECT
    unidade.nr_seq_interno AS nr_seq_interno
  , {{ fill_null_bigint('unidade.nr_atendimento', -1) }} AS nr_atendimento
  , {{ fill_null_bigint('unidade.nr_sequencia', -1) }} AS nr_sequencia
  , {{ fill_null_bigint('unidade.cd_setor_atendimento', -1) }} AS cd_setor_atendimento
  , {{ fill_null_bigint('unidade.cd_unidade_basica', -1) }} AS cd_unidade_basica
  , {{ fill_null_bigint('unidade.cd_unidade_compl', -1) }} AS cd_unidade_compl
  , {{ standardize_timestamp('unidade.dh_entrada_unidade') }} AS dh_entrada_unidade
  , {{ standardize_timestamp('unidade.dh_atualizacao') }} AS dh_atualizacao
  , {{ standardize_text_initcap('unidade.nm_usuario', "'indefinido'") }} AS nm_usuario
  , {{ fill_null_bigint('unidade.cd_tipo_acomodacao', -1) }} AS cd_tipo_acomodacao
  , {{ standardize_timestamp('unidade.dh_saida_unidade') }} AS dh_saida_unidade
  , {{ fill_null_bigint('unidade.nr_atend_dia', -1) }} AS nr_atend_dia
  , {{ standardize_text_initcap('unidade.ds_observacao', "'indefinido'") }} AS ds_observacao
  , {{ standardize_text_initcap('unidade.nm_usuario_original', "'indefinido'") }} AS nm_usuario_original
  , {{ standardize_timestamp('unidade.dh_saida_interno') }} AS dh_saida_interno
  , {{ standardize_enum('unidade.ie_passagem_setor', "'i'") }} AS ie_passagem_setor
  , {{ fill_null_bigint('unidade.nr_acompanhante', -1) }} AS nr_acompanhante
  , {{ standardize_enum('unidade.ie_calcular_dif_diaria', "'i'") }} AS ie_calcular_dif_diaria
  , {{ fill_null_bigint('unidade.nr_seq_motivo_transf', -1) }} AS nr_seq_motivo_transf
  , {{ standardize_timestamp('unidade.dh_entrada_real') }} AS dh_entrada_real
  , {{ standardize_text_initcap('unidade.nm_usuario_real', "'indefinido'") }} AS nm_usuario_real
  , {{ standardize_enum('unidade.ie_radiacao', "'i'") }} AS ie_radiacao
  , {{ fill_null_bigint('unidade.nr_seq_motivo_dif', -1) }} AS nr_seq_motivo_dif
  , {{ fill_null_bigint('unidade.nr_seq_mot_dif_diaria', -1) }} AS nr_seq_mot_dif_diaria
  , {{ fill_null_bigint('unidade.cd_unidade_externa', -1) }} AS cd_unidade_externa
  , {{ standardize_timestamp('unidade.dh_alta_medico_setor') }} AS dh_alta_medico_setor
  , {{ fill_null_bigint('unidade.cd_motivo_alta_setor', -1) }} AS cd_motivo_alta_setor
  , {{ fill_null_bigint('unidade.cd_procedencia_setor', -1) }} AS cd_procedencia_setor
  , {{ fill_null_bigint('unidade.nr_seq_motivo_int', -1) }} AS nr_seq_motivo_int
  , {{ fill_null_bigint('unidade.nr_seq_motivo_int_sub', -1) }} AS nr_seq_motivo_int_sub
  , {{ fill_null_bigint('unidade.nr_seq_motivo_perm', -1) }} AS nr_seq_motivo_perm
  , {{ standardize_timestamp('unidade.dh_atualizacao_nrec') }} AS dh_atualizacao_nrec
  , {{ standardize_text_initcap('unidade.nm_usuario_nrec', "'indefinido'") }} AS nm_usuario_nrec
  , {{ fill_null_bigint('unidade.nr_cirurgia', -1) }} AS nr_cirurgia
  , {{ fill_null_bigint('unidade.nr_seq_pepo', -1) }} AS nr_seq_pepo
  , {{ fill_null_bigint('unidade.nr_seq_agrupamento', -1) }} AS nr_seq_agrupamento
  , {{ normalize_decimal('unidade.qt_tempo_prev', 2, -1) }} AS qt_tempo_prev
  , {{ fill_null_bigint('unidade.nr_seq_unid_ant', -1) }} AS nr_seq_unid_ant
  , {{ standardize_timestamp('unidade.dh_saida_temporaria') }} AS dh_saida_temporaria
  , {{ standardize_timestamp('unidade.dh_retorno_saida_temporaria') }} AS dh_retorno_saida_temporaria
  , {{ fill_null_string('unidade.id_leito_temp_cross', "'ind'") }} AS id_leito_temp_cross
  , {{ fill_null_bigint('unidade.cd_departamento', -1) }} AS cd_departamento
  , {{ fill_null_bigint('unidade.cd_procedencia_atend', -1) }} AS cd_procedencia_atend
  , {{ fill_null_bigint('unidade.nr_seq_classif_esp', -1) }} AS nr_seq_classif_esp
  , {{ standardize_enum('unidade.ie_anzics_generated', "'i'") }} AS ie_anzics_generated
  , {{ fill_null_bigint('unidade.cd_evolucao', -1) }} AS cd_evolucao
  , {{ standardize_timestamp('unidade.dh_est_return') }} AS dh_est_return
  , {{ fill_null_bigint('atend.cd_pessoa_fisica', -1) }} AS atend_cd_pessoa_fisica
  , {{ standardize_enum('atend.ie_tipo_atendimento', "'i'") }} AS atend_ie_tipo_atendimento
  , {{ standardize_date('atend.dt_entrada') }} AS atend_dt_entrada
  , {{ standardize_date('atend.dt_alta') }} AS atend_dt_alta
  , {{ standardize_enum('atend.ie_status_atendimento', "'i'") }} AS atend_ie_status_atendimento
  , {{ fill_null_bigint('atend.cd_medico_resp', -1) }} AS atend_cd_medico_resp
  , {{ standardize_enum('atend.ie_clinica', "'i'") }} AS atend_ie_clinica
  , {{ fill_null_bigint('atend.cd_estabelecimento', -1) }} AS atend_cd_estabelecimento
  , {{ fill_null_bigint('atend.cd_procedencia', -1) }} AS atend_cd_procedencia
  , {{ fill_null_bigint('atend.cd_motivo_alta', -1) }} AS atend_cd_motivo_alta
  {{ silver_context_audit_columns() }}

FROM unidade
LEFT JOIN atend
  ON unidade.nr_atendimento = atend.nr_atendimento
