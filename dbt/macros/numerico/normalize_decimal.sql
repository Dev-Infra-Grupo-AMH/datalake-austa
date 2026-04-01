{#- DECIMAL: ROUND to scale; NULL → decimal sentinel. -#}
{% macro normalize_decimal(expression, scale, sentinel) -%}
COALESCE(
  ROUND(CAST({{ expression }} AS DECIMAL(38, 6)), {{ scale }}),
  CAST({{ sentinel }} AS DECIMAL(38, 2))
)
{%- endmacro %}
