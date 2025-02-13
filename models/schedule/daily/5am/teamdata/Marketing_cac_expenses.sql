
select e.d,b.date as mois,country,levier ,cat1 as levierFC,upper(cat2) as type,
case when upper(cat3) like 'ACQ%' then 'ACQUIZ'else upper(Cat3) end  as type2,
'Oui' as cac,sum(spent) as spent 
FROM `teamdata-291012.marketing.live_expenses`e
inner join inter.boxes_by_day b on b.d=e.d and b.dw_country_code=e.country
 where cat2 <>'Shop'
group by 1,2,3,4,5,6,7

union all
sELECT period,b.date,'FR' ,'splio','TOOLS','BOX','ACQUIZ','Oui',8000/31 
FROM UNNEST(
  GENERATE_DATE_ARRAY(
    ('2023-01-01'),
    ('2024-12-31')
  )
) period
inner join inter.boxes_by_day b on b.d=period and b.dw_country_code='FR'

union all
sELECT period,b.date,'DE' ,'splio','TOOLS','BOX','ACQUIZ','Oui',550/31 FROM UNNEST(
  GENERATE_DATE_ARRAY(
    ('2023-01-01'),
    ('2024-12-31')
  )
) period
inner join inter.boxes_by_day b on b.d=period and b.dw_country_code='FR'

union all
sELECT period,b.date,'FR' ,'KolsQUare','TOOLS','BOX','ACQUIZ','Oui',1000/31 
FROM UNNEST(
  GENERATE_DATE_ARRAY(
    ('2023-01-01'),
    ('2024-12-31')
  )
) period
inner join inter.boxes_by_day b on b.d=period and b.dw_country_code='FR'

union all
sELECT period,b.date,'FR' ,'Arcane','TOOLS','BOX','ACQUIZ','Oui',800/31
 FROM UNNEST(
  GENERATE_DATE_ARRAY(
    ('2023-01-01'),
    ('2024-12-31')
  )
) period
inner join inter.boxes_by_day b on b.d=period and b.dw_country_code='FR'


---------------
-----influence
---------------



union all
SELECT e.date,b.date,pays,nom,type,'BOX','ACQUIZ','Oui',sum(spent)
 FROM `teamdata-291012.marketing.influence_expense` e
inner join inter.boxes_by_day b on b.d=e.date and b.dw_country_code=pays
where box='Box'
group by 1,2,3,4,5,6,7

---------------
-----Manual
---------------



union all
SELECT e.date,b.date,pays,levier,levierFC,'BOX','ACQUIZ',cac,sum(spent) FROM `teamdata-291012.marketing.manual_expense` e
inner join inter.boxes_by_day b on b.d=e.date and b.dw_country_code=pays
group by 1,2,3,4,5,6,7,8




---------------
-----YEARLY
---------------

union all 


SELECT 
       DATE(payment_date) AS d,
       b.date,
       bs.dw_country_code,'yearly','Discount','BOX','ACQUIZ','Oui',
       SUM(discount) AS discount
FROM sales.box_sales bs
inner join inter.boxes_by_day b on b.d= DATE(payment_date) and b.dw_country_code=bs.dw_country_code
WHERE yearly = 1
AND bs.dw_country_code IN ('FR', 'DE')
AND sub_payment_status_id = 1
GROUP BY 1,2,3,4,5,6

---------------
-----Discount
---------------
union all
(
WITH raf_by_sub AS (
  SELECT rsl.dw_country_code AS country_code,
         rsl.order_detail_sub_id AS sub_id,
         rod.reward_value
  FROM inter.raf_sub_link rsl
  INNER JOIN inter.raf_offer_details rod ON rsl.dw_country_code = rod.dw_country_code AND rsl.raf_offer_detail_id = rod.id
  INNER JOIN inter.raf_offers ro ON rod.dw_country_code = ro.dw_country_code AND rod.raf_offer_id = ro.id
  INNER JOIN inter.raf_reward_type rrt ON ro.dw_country_code = rrt.dw_country_code AND ro.refferer_reward_type = rrt.id
  WHERE rrt.reward_type = 'BOX'
)
SELECT payment_date,b.date,t.dw_country_code AS country_code,
       
       coupon,
       'Discount',
       'BOX',
       'ACQUIZ','Oui',
       SUM(discount_wout_raf) AS total_discount_wout_raf,

FROM
(
  SELECT
      bs.dw_country_code,
      bs.year,
      bs.month,
      DATE(bs.payment_date) AS payment_date,
      b.shipping_date,
      bs.sub_id, 
      bs.user_id,
      bs.box_id,
      bs.date,
      bs.discount - COALESCE(MAX(rbs.reward_value) / (1 + MAX(bs.vat_rate) / 100), 0) AS discount_wout_raf,
      COALESCE(MAX(rbs.reward_value) / (1 + MAX(bs.vat_rate) / 100), 0) AS raf_discount,
      CASE WHEN bs.box_id = bs.sub_start_box THEN bs.coupon_code ELSE bs.sub_offer_code END AS coupon,
      CASE WHEN max(bs1.box_id) = bs.box_id - 1 THEN 'react m-1'
           WHEN bs.gift = 1 THEN 'acquis gift'
           WHEN bs1.user_id > 0 THEN 'react self'
           ELSE 'acquis self'
           END AS sub_type
      FROM sales.box_sales bs
      LEFT JOIN sales.box_sales bs1 ON bs1.user_id = bs.user_id AND bs1.box_id < bs.box_id AND bs.dw_country_code = bs1.dw_country_code
      LEFT JOIN raf_by_sub rbs ON bs.dw_country_code = rbs.country_code AND bs.sub_id = rbs.sub_id
      INNER JOIN inter.boxes b ON b.id = bs.box_id AND b.dw_country_code = bs.dw_country_code
      WHERE bs.payment_date >= b.shipping_date
      AND bs.payment_status = 'paid'
      AND (bs.dw_country_code NOT IN ('FR', 'DE') OR bs.coupon_code <> 'YEARLY')
      AND bs.date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 YEAR)
      GROUP BY bs.dw_country_code, bs.year, bs.month, bs.date, bs.sub_id, bs.payment_date, bs.user_id, bs.box_id, b.shipping_date, bs.gift, bs1.user_id, bs.sub_start_box, bs.coupon_code, bs.sub_offer_code, bs.discount
) t
LEFT JOIN inter.coupons c ON c.code = t.coupon AND c.dw_country_code = t.dw_country_code
LEFT JOIN inter.products p ON c.discount_type = 'PRODUCT' AND c.discount_amount = CAST(p.id AS STRING)and p.dw_country_code=c.dw_country_code
left join (select distinct country,code,type,type2 from `teamdata-291012.marketing.Marketing_cac_discount` )d on coupon=d.code and type2='Box' and country=t.dw_country_code
inner join inter.boxes_by_day b on b.d= DATE(payment_date) and b.dw_country_code=t.dw_country_code
WHERE sub_type <> 'acquis gift' 
GROUP BY 1,2,3,4,5,6)



---------------
-----GWS
---------------


union all
(
WITH raf_by_sub AS (
  SELECT rsl.dw_country_code AS country_code,
         rsl.order_detail_sub_id AS sub_id,
         rod.reward_value
  FROM inter.raf_sub_link rsl
  INNER JOIN inter.raf_offer_details rod ON rsl.dw_country_code = rod.dw_country_code AND rsl.raf_offer_detail_id = rod.id
  INNER JOIN inter.raf_offers ro ON rod.dw_country_code = ro.dw_country_code AND rod.raf_offer_id = ro.id
  INNER JOIN inter.raf_reward_type rrt ON ro.dw_country_code = rrt.dw_country_code AND ro.refferer_reward_type = rrt.id
  WHERE rrt.reward_type = 'BOX'
)
SELECT payment_date,b.date,t.dw_country_code AS country_code,
       
       concat(d.type),
       'GWS',
       'GWS',
       'ACQUIZ',
      'Oui',
       COUNT(*) * MAX(COALESCE(p.attr_discounted_purchase_price, p.attr_purchase_price)) AS total_discount_wout_raf,

FROM
(
  SELECT
      bs.dw_country_code,
      bs.year,
      bs.month,
      DATE(bs.payment_date) AS payment_date,
      b.shipping_date,
      bs.sub_id, 
      bs.user_id,
      bs.box_id,
      bs.date,
      bs.discount - COALESCE(MAX(rbs.reward_value) / (1 + MAX(bs.vat_rate) / 100), 0) AS discount_wout_raf,
      COALESCE(MAX(rbs.reward_value) / (1 + MAX(bs.vat_rate) / 100), 0) AS raf_discount,
      CASE WHEN bs.box_id = bs.sub_start_box THEN bs.coupon_code ELSE bs.sub_offer_code END AS coupon,
      CASE WHEN max(bs1.box_id) = bs.box_id - 1 THEN 'react m-1'
           WHEN bs.gift = 1 THEN 'acquis gift'
           WHEN bs1.user_id > 0 THEN 'react self'
           ELSE 'acquis self'
           END AS sub_type
      FROM sales.box_sales bs
      LEFT JOIN sales.box_sales bs1 ON bs1.user_id = bs.user_id AND bs1.box_id < bs.box_id AND bs.dw_country_code = bs1.dw_country_code
      LEFT JOIN raf_by_sub rbs ON bs.dw_country_code = rbs.country_code AND bs.sub_id = rbs.sub_id
      INNER JOIN inter.boxes b ON b.id = bs.box_id AND b.dw_country_code = bs.dw_country_code
      WHERE bs.payment_date >= b.shipping_date
      AND bs.payment_status = 'paid'
      AND (bs.dw_country_code NOT IN ('FR', 'DE') OR bs.coupon_code <> 'YEARLY')
      AND bs.date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 YEAR)
      GROUP BY bs.dw_country_code, bs.year, bs.month, bs.date, bs.sub_id, bs.payment_date, bs.user_id, bs.box_id, b.shipping_date, bs.gift, bs1.user_id, bs.sub_start_box, bs.coupon_code, bs.sub_offer_code, bs.discount
) t
LEFT JOIN inter.coupons c ON c.code = t.coupon AND c.dw_country_code = t.dw_country_code
LEFT JOIN inter.products p ON c.discount_type = 'PRODUCT' AND c.discount_amount = CAST(p.id AS STRING)and p.dw_country_code=c.dw_country_code
left join (select distinct country,code,type,type2 from `teamdata-291012.marketing.Marketing_cac_discount`)d on coupon=d.code and country=t.dw_country_code
inner join inter.boxes_by_day b on b.d= DATE(payment_date) and b.dw_country_code=t.dw_country_code
WHERE sub_type <> 'acquis gift' and coupon is not null
GROUP BY 1,2,3,4,5,6
ORDER BY country_code, payment_date, COUNT(*) DESC)