{{ config(
    materialized='incremental',
    unique_key='archive_date',
    partition_by={
      "field": "archive_date", 
      "data_type": "date"
    },
    schema='history_table'
) }}

SELECT
  CURRENT_DATE('Europe/Paris') AS archive_date,
  p.* EXCEPT(purchase_price_depreciated, date_depreciation, first_product_type, brand_flag, picture_link)
  REPLACE(
    SAFE_CAST(perceived_price AS FLOAT64) AS perceived_price
  )
FROM `teamdata-291012.catalog.bank` AS p

{% if is_incremental() %}
  WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} h 
    WHERE h.archive_date = CURRENT_DATE('Europe/Paris')
  )
{% endif %}