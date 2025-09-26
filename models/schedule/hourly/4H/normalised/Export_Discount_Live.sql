{{
  config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date"
    },
    cluster_by=["dw_country_code"]
  )
}}

SELECT 
    bs.dw_country_code,
    bs.date as month, 
    date(payment_date) as date,
    round(sum(bs.discount),1) as cost,
    case 
        when coupon is null and raffed=1 then 'RAF' 
        else coupon 
    end as coupon,
    case 
        when yearly=1 then 'YEARLY'
        when raffed=1 then 'RAF' 
        else 'DISCOUNT' 
    end as type,
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
WHERE diff_current_box <= 0 
    AND (coupon is not null or raffed = 1) and bs.year>2021
GROUP BY ALL 
HAVING cost > 0 
