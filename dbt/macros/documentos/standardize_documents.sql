{#-
  Numeric document string: strip non-digits; if length < expected_length, left-pad with zeros.
  expected_length: e.g. 11 (CPF) or 14 (CNPJ).
-#}
{% macro standardize_documents(expression, expected_length) -%}
CASE
  WHEN {{ expression }} IS NULL THEN CAST(NULL AS STRING)
  WHEN LENGTH(REGEXP_REPLACE(TRIM(CAST({{ expression }} AS STRING)), '[^0-9]', '')) = {{ expected_length }} THEN
    REGEXP_REPLACE(TRIM(CAST({{ expression }} AS STRING)), '[^0-9]', '')
  WHEN LENGTH(REGEXP_REPLACE(TRIM(CAST({{ expression }} AS STRING)), '[^0-9]', '')) BETWEEN 1 AND ({{ expected_length }} - 1) THEN
    LPAD(REGEXP_REPLACE(TRIM(CAST({{ expression }} AS STRING)), '[^0-9]', ''), {{ expected_length }}, '0')
  ELSE REGEXP_REPLACE(TRIM(CAST({{ expression }} AS STRING)), '[^0-9]', '')
END
{%- endmacro %}
