{{ 
  config(
    materialized = 'incremental',
    unique_key = ['archive_date', 'period', 'key'],
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      'field': 'archive_date',
      'data_type': 'date'
    }
  ) 
}}

SELECT current_date AS archive_date, period, key, value 
FROM `teamdata-291012.pnl.export_pnl_live`
{% if is_incremental() %}
WHERE CURRENT_DATE() > (SELECT MAX(archive_date) FROM {{ this }})
{% endif %}