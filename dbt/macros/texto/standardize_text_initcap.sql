{#- Free text: INITCAP after empty → default literal. -#}
{% macro standardize_text_initcap(expression, literal_sql) -%}
INITCAP({{ fill_null_string(expression, literal_sql) }})
{%- endmacro %}
