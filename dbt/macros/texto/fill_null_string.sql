{#- NULL or blank after TRIM → SQL literal (pass quoted, e.g. 'indefinido'). -#}
{% macro fill_null_string(expression, literal_sql) -%}
COALESCE(NULLIF(TRIM(CAST({{ expression }} AS STRING)), ''), {{ literal_sql }})
{%- endmacro %}
