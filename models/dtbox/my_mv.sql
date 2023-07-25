{{
  config(
    materialized = 'materialized_view'
    )
}}

SELECT * FROM {{ ref('my_view') }}