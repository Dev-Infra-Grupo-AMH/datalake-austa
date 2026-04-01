{#- Enumerated / flag columns: LOWER + empty → default literal (e.g. 'i'). -#}
{% macro standardize_enum(expression, literal_sql) -%}
LOWER(COALESCE(NULLIF(TRIM(CAST({{ expression }} AS STRING)), ''), {{ literal_sql }}))
{%- endmacro %}
