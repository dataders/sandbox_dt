{{
  config(
    materialized = 'view',
    )
}}
SELECT * FROM {{ ref('my_fake_data') }}
WHERE DATETIME <= SYSDATE()
{# WHERE datetime <= LOCAL_TIMESTAMP() -- redshift #}
{# WHERE datetime <= CURRENT_TIMESTAMP() -- snowflake #}