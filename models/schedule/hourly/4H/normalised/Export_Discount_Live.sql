{{
  config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date",
       "granularity": "month"
      
    },
    cluster_by=["dw_country_code"]
  )
}}

WITH subs_with_real_coupon AS 
(
  SELECT
  bs.discount,
  bs.sub_id,
  bs.date AS month,
  CASE WHEN day_in_cycle >=0 THEN b.shipping_date ELSE date(payment_date) END as date,
    bs.dw_country_code,
    b.shipping_date AS first_date,
    bs.order_id,
    bs.box_id,
    bs.yearly,
    COALESCE(
      bs.coupon,
      CASE WHEN raffed=1 OR rsl.order_detail_sub_id IS NOT NULL THEN 'RAF'ELSE NULL END,
      LAST_VALUE(coupon IGNORE NULLS) OVER (
        PARTITION BY bs.dw_country_code, bs.order_id
        ORDER BY box_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
    ) AS coupon_filled
  FROM sales.box_sales bs
  JOIN inter.boxes b ON b.date = bs.date AND b.dw_country_code = bs.dw_country_code
  LEFT JOIN `teamdata-291012.inter.raf_sub_link` rsl ON rsl.dw_country_code = bs.dw_country_code AND bs.sub_id = rsl.order_detail_sub_id
) 

SELECT dw_country_code, month,  date, ROUND(SUM(discount)) AS cost,
coupon_filled AS coupon,
CASE WHEN yearly = 1 THEN 'YEARLY' 
WHEN coupon_filled = 'RAF' THEN 'RAF'
ELSE 'DISCOUNT' END AS type,
 'BOX' as product_type,
    'ACQUIZ' as acquis_type,
    first_date as first_day 
FROM subs_with_real_coupon
WHERE discount > 0
GROUP BY ALL
