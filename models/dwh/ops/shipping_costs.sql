WITH base_data AS (
  SELECT shipping_mode_id, price_ht AS price, min_weight, max_weight, date_start, price_daily_ht AS price_daily, shipping_taxes_rate
  FROM `update_table.shipping_costs`
  WHERE shipping_mode_id IS NOT NULL
),
all_shipping_date_prices AS (
  SELECT shipping_mode_id, date_start, max(shipping_taxes_rate) AS shipping_taxes_rate
  FROM base_data
  GROUP BY shipping_mode_id, date_start
),
date_end_table AS (
  SELECT shipping_mode_id,
         date_start,
         DATE_SUB(LEAD(date_start) OVER (PARTITION BY shipping_mode_id ORDER BY date_start), INTERVAL 1 DAY) AS date_end,
         shipping_taxes_rate
  FROM all_shipping_date_prices
)

SELECT bd.shipping_mode_id, max(bd.price) price, bd.min_weight, bd.max_weight, max(bd.price_daily) price_daily,  bd.date_start,
det.date_end, max(COALESCE(det.shipping_taxes_rate,lc.price)) AS shipping_taxes_rate
FROM base_data bd
LEFT JOIN date_end_table det USING(shipping_mode_id, date_start)
LEFT JOIN {{ ref('logistics_costs') }} lc ON lc.name = 'shipping_taxes_rate' AND lc.date_start<= bd.date_start AND (lc.date_end >= bd.date_start OR lc.date_end IS NULL)
group by all
