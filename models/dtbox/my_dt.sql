{{
  config(
    materialized = 'dynamic_table',
    target_lag = '1 minute',
    snowflake_warehouse = 'DBT_TESTING'
    )
}}

SELECT * FROM {{ ref('my_view') }}