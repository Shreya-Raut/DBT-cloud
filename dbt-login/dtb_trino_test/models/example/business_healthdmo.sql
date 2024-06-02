{{ config(
        materialized='incremental',
        database='lakehouse_dev',
        schema='bi_dwh_ref_data_reporting',
        incremental_strategy='append',
        on_table_exists = 'drop',
        views_enabled=false,
        alias='business_health',
        properties={
          "partitioned_by": "ARRAY['year_month_day', 'date_scope', 'region']",
        }
    )
}}
{{ sales() }}
union
{{ derived() }}
union
{{ transactions() }}
union
{{ customer() }}