{{
  config(
    materialized='table',
    schema='marketing',
    partition_by={
      "field": "daily_date",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=["dw_country_code"]
  )
}}

WITH costs AS (
SELECT e.*, b.shipping_date, b.closing_date
FROM `teamdata-291012.Spreadsheet_synchro.input_special_marketing_expenses` e
JOIN inter.boxes b ON b.dw_country_code = e.country AND b.date = e.box
WHERE e.cost IS NOT NULL
)

SELECT
  t.country AS dw_country_code,  -- ⚠️ MODIFIÉ ICI
  t.box,
  t.market,
  t.product_type,
  t.supplier,
  t.channel,
  t.cost,
  day AS daily_date,
  t.cost / (DATE_DIFF(t.closing_date, t.shipping_date, DAY) + 1) AS daily_cost
FROM
  costs t,
  UNNEST(GENERATE_DATE_ARRAY(t.shipping_date, t.closing_date)) AS day