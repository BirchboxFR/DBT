{{ config(
    materialized='incremental',
    unique_key='archive_date',
    partition_by={
      "field": "archive_date", 
      "data_type": "date"
    },
    schema='history_table'
) }}



SELECT current_date AS archive_date,  
s.*
 
 FROM `teamdata-291012.ops.ruptures_longues` s
