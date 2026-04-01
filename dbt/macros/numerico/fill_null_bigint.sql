{#- BIGINT: NULL → sentinel (e.g. -1). -#}
{% macro fill_null_bigint(expression, sentinel) -%}
COALESCE({{ expression }}, CAST({{ sentinel }} AS BIGINT))
{%- endmacro %}
