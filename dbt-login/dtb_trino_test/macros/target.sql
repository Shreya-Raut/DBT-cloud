{% macro my_target_table(t_database, t_schema, t_alias) %}
{{ config(
      materialized='table',
      database=t_database,
      schema=t_schema,
      alias=t_alias
  ) }}
{% endmacro %}