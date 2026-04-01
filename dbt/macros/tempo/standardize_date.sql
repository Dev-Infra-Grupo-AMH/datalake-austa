{#- DATE: NULL, invalid cast, or year >= 2900 → 1900-01-01. -#}
{% macro standardize_date(expression) -%}
CASE
  WHEN {{ expression }} IS NULL THEN DATE '1900-01-01'
  WHEN TRY_CAST({{ expression }} AS DATE) IS NULL THEN DATE '1900-01-01'
  WHEN YEAR(TRY_CAST({{ expression }} AS DATE)) >= 2900 THEN DATE '1900-01-01'
  ELSE TRY_CAST({{ expression }} AS DATE)
END
{%- endmacro %}
