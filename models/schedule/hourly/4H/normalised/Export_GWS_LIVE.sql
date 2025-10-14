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

SELECT 
    bs.dw_country_code,
    bs.date as month, 
    date(payment_date) as date,
    round(sum(COALESCE(p.attr_discounted_purchase_price, p.attr_purchase_price)),1) as cost,
    coupon,
    'Discount' as type,
    'BOX' as product_type,
    'ACQUIZ' as acquis_type,
    shipping_Date as first_day
FROM sales.box_sales bs 
LEFT JOIN inter.coupons c 
    ON c.code = bs.coupon 
    AND c.dw_country_code = bs.dw_country_code 
LEFT JOIN inter.products p 
    ON c.discount_type = 'PRODUCT' 
    AND c.discount_amount = CAST(p.id AS STRING)
    AND p.dw_country_code = c.dw_country_code
JOIN inter.boxes b 
    ON b.dw_country_code = bs.dw_country_code 
    AND b.date = bs.date 
WHERE coupon is not null and bs.year>2021
GROUP BY ALL 
HAVING cost > 0 
