{{ config(
        materialized='incremental',
        database='lakehouse_dev',
        schema='bi_dwh_ref_data_reporting',
        incremental_strategy='append',
        on_schema_change='append_new_column',
        views_enabled=false,
        alias='business_health_demo',
        properties={
          "format": "'PARQUET'",
          "partitioning": "ARRAY['year_month_day','metric_name']",
        }
        )
}}
{{ transactions() }}
union
{{ customer() }}
union
{{ sales() }}
union
{{ derived() }}

