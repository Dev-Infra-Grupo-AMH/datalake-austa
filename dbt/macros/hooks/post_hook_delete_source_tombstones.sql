{#-
  Pós-hook incremental: DELETE na própria tabela do modelo onde o PK ainda existe na bronze com tombstone.

  O SQL não pode ser totalmente resolvido na 1ª renderização do arquivo (config/post_hook): nessa hora
  `model.schema` ainda é o do profiles (ex.: raw). O parser do dbt re-renderiza o hook *depois* de
  `update_parsed_node_relation_names`, desde que o texto ainda contenha Jinja.

  Por isso devolvemos literais `{{ this }}` e `{{ ref('...') }}` na string (via `'{{ ... }}'` em Jinja),
  para a 2ª passagem expandir com o schema/relacionamento corretos. `key_column` e `deleted_column`
  são fragmentos SQL seguros passados como strings e podem ser interpolados na 1ª passagem.
-#}
{% macro post_hook_delete_source_tombstones(source_model, key_column, deleted_column='__deleted') %}
  {% set sql %}
DELETE FROM {{ '{{ this }}' }} WHERE {{ key_column }} IN (
  SELECT {{ key_column }} FROM {{ '{{ ref(\'' ~ source_model ~ '\') }}' }} WHERE {{ deleted_column }} = true
)
  {% endset %}
  {{ return(sql | trim) }}
{% endmacro %}
