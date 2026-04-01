{#- TIMESTAMP: NULL, invalid cast, or year >= 2900 → 1900-01-01 00:00:00. -#}
{% macro standardize_timestamp(expression) -%}
CASE
  WHEN {{ expression }} IS NULL THEN TIMESTAMP '1900-01-01 00:00:00'
  WHEN TRY_CAST({{ expression }} AS TIMESTAMP) IS NULL THEN TIMESTAMP '1900-01-01 00:00:00'
  WHEN YEAR(TRY_CAST({{ expression }} AS TIMESTAMP)) >= 2900 THEN TIMESTAMP '1900-01-01 00:00:00'
  ELSE TRY_CAST({{ expression }} AS TIMESTAMP)
END
{%- endmacro %}
