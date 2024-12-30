SELECT dw_country_code,
       year,
       month,
       FORMAT_DATE('%Y-%m', date) AS m,
       box_id,
       payment_date,
       shipping_date AS openning_cycle_date,
       DATE_DIFF(payment_date, shipping_date, DAY) + 1 AS nb_days_since_opening, 
       sub_type,
       COUNT(*) AS nb_subs
FROM
(
    SELECT bs.dw_country_code,
           bs.year,
           bs.month,
           DATE(bs.payment_date) AS payment_date,
           b.shipping_date,
           bs.sub_id, 
           bs.user_id,
           bs.box_id,
           bs.date,
           CASE WHEN MAX(bs1.box_id) = bs.box_id - 1 THEN 'react m-1'
                WHEN bs.gift = 1 THEN 'acquis gift'
                WHEN bs1.user_id > 0 THEN 'react self'
                ELSE 'acquis self'
           END AS sub_type
    FROM {{ ref('box_sales') }} bs
    LEFT JOIN {{ ref('box_sales') }} bs1 ON bs.dw_country_code = bs1.dw_country_code AND bs1.user_id = bs.user_id AND bs1.box_id < bs.box_id
    INNER JOIN `inter.boxes` b ON bs.dw_country_code = b.dw_country_code AND b.id = bs.box_id
    WHERE bs.payment_date >= b.shipping_date
    AND bs.payment_status = 'paid'
    GROUP BY bs.dw_country_code,
             bs.year,
             bs.month,
             bs.date,
             bs.sub_id,
             bs.payment_date,
             bs.user_id,
             bs.box_id,
             b.shipping_date,
             bs.gift,
             bs1.user_id
) t
WHERE sub_type <> 'react m-1'
GROUP BY dw_country_code,
         year,
         month,
         box_id,
         shipping_date,
         payment_date,
         sub_type,
         m
