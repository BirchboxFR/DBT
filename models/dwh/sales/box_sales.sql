{{ config(
    materialized='table',
    on_schema_change='ignore' ,
    partition_by={
      "field": "box_id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 3000,
        "interval": 1
      }
    },
    cluster_by=['dw_country_code', 'date','order_id']
) }}



WITH 
sub_suspend_survey_reason AS
(
SELECT * EXCEPT (row_num)
FROM
(
SELECT sr.dw_country_code, sr.customer_id AS user_id, b.id AS box_id, sqa.title AS survey_reason, ROW_NUMBER() OVER (PARTITION BY sr.dw_country_code, sr.customer_id, b.id ORDER BY sr.answered_at DESC) AS row_num
FROM {{ ref('sub_suspend_survey_result') }} sr
JOIN {{ ref('boxes') }} b ON b.dw_country_code = sr.dw_country_code AND b.id = sr.last_received_box_id + 1
JOIN {{ ref('sub_suspend_survey_result_answer') }} sra ON sra.result_id = sr.result_id AND sr.dw_country_code = sra.dw_country_code
JOIN {{ ref('sub_suspend_survey_question_answer') }} sqa ON sqa.question_answer_id = sra.question_answer_id and sqa.dw_country_code=sra.dw_country_code
WHERE sra.question_id = 1
) t
WHERE row_num = 1
),
products as (
select 
box_id,coffret_id,dw_country_code,max(inventory_item_id) inventory_item_id,
product_codification_id,max(id) as id 
from {{ ref('products') }} 

group by all
),
shipping_mode_dedup as(
select 
shipping_mode_id,max(price)price,min_weight,max_weight,max(price_daily) price_daily,date_Start,date_end,max(shipping_taxes_rate)shipping_taxes_rate
from {{ ref('shipping_costs') }}
group by all
),
ranked_sub_history AS
(
  SELECT o.dw_country_code, o.user_id, sh.box_id, FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',sh.timestamp) AS d,
  CASE 
    WHEN ssr.value IN ('Too many fails','Card expired', 'Breakage') THEN 'technical'
    WHEN ssr.value IN ('Self suspended', 'Paused', 'Paused for gift') THEN 'self-willed'
    ELSE NULL
  END AS sub_suspended_reason_lvl1, 
  CASE 
    WHEN ssr.value ='Card expired' THEN 'expired card'
    WHEN ssr.value IN ('Self suspended') THEN 'suspended'
    WHEN ssr.value IN ('Paused', 'Paused for gift') THEN 'paused'
    WHEN ssr.value IN ('Too many fails','Breakage') THEN 'breakage'
    ELSE NULL
  END AS sub_suspended_reason_lvl2, 
  'reason from survey' AS sub_suspended_reason_lvl3,
  ROW_NUMBER() OVER (PARTITION BY o.user_id, sh.box_id, sh.dw_country_code ORDER BY timestamp DESC) AS row_num
  FROM {{ ref('sub_history') }} sh
  JOIN {{ ref('order_detail_sub') }} s ON s.order_detail_id = sh.order_detail_id AND s.box_id = sh.box_id AND sh.dw_country_code = s.dw_country_code
  JOIN {{ ref('order_details') }} d ON d.id = s.order_detail_id AND d.dw_country_code = s.dw_country_code
  JOIN {{ ref('orders') }} o ON o.id = d.order_id AND d.dw_country_code = o.dw_country_code
  JOIN {{ ref('sub_suspended_reasons') }} ssr ON ssr.dw_country_code = sh.dw_country_code AND ssr.id = sh.sub_suspended_reasons_id

  
  AND sh.action = -1
  
),
sub_history_reasons AS
(
  SELECT * EXCEPT(row_num)
  FROM ranked_sub_history
  WHERE row_num = 1
),
adyen_ranked AS
(
  SELECT an.dw_country_code, o.user_id, s.box_id, 
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',an.eventDate) as d,
  'technical' AS sub_suspended_reason_lvl1,
  CASE WHEN an.reason LIKE '%xpired%' THEN 'expired card' ELSE 'breakage' END AS sub_suspended_reason_lvl2,
  an.reason AS sub_suspended_reason_lvl3,
  ROW_NUMBER() OVER (PARTITION BY o.user_id, s.box_id, s.dw_country_code ORDER BY an.eventDate DESC) AS row_num
  FROM `inter.adyen_notifications` an
  JOIN {{ ref('order_detail_sub') }} s ON s.id = an.sub_id AND s.dw_country_code = an.dw_country_code
  JOIN {{ ref('order_details') }} d ON d.id = s.order_detail_id AND d.dw_country_code = s.dw_country_code
  JOIN {{ ref('orders') }} o ON o.id = d.order_id AND o.dw_country_code = d.dw_country_code
  WHERE an.success = 0
  
),
adyen_reasons AS
(
  SELECT * EXCEPT(row_num)
  FROM adyen_ranked
  WHERE row_num = 1
),
all_reasons AS
(
   SELECT sr.*
  FROM sub_history_reasons sr
  LEFT JOIN adyen_reasons an ON an.dw_country_code = sr.dw_country_code AND an.user_id = sr.user_id AND an.box_id = sr.box_id
  WHERE an.user_id IS NULL
  UNION ALL
  SELECT *
  FROM adyen_reasons an 
),
all_reasons_ranked AS
(
  SELECT all_reasons.dw_country_code, all_reasons.user_id, all_reasons.box_id, all_reasons.sub_suspended_reason_lvl1, all_reasons.sub_suspended_reason_lvl2, 
  CASE WHEN sub_suspended_reason_lvl3 = 'reason from survey' THEN ssr.survey_reason ELSE sub_suspended_reason_lvl3 END AS sub_suspended_reason_lvl3,
  ROW_NUMBER() OVER (PARTITION BY all_reasons.user_id, all_reasons.box_id, all_reasons.dw_country_code ORDER BY d DESC) AS row_num
  FROM all_reasons
  LEFT JOIN sub_suspend_survey_reason ssr ON ssr.dw_country_code = all_reasons.dw_country_code AND ssr.user_id = all_reasons.user_id AND ssr.box_id = all_reasons.box_id
),

self_churn_reason AS
(
  SELECT * EXCEPT(row_num)
  FROM all_reasons_ranked
  WHERE row_num = 1
),
 gws_costs_table AS (
  SELECT sol.dw_country_code,
         sol.sub_id,
         COALESCE(SUM(c.purchase_price * d.quantity), 0) AS gws_costs
  FROM {{ ref('sub_order_link') }} sol
  INNER JOIN {{ ref('orders') }} o ON sol.dw_country_code = o.dw_country_code AND sol.order_id = o.id
  INNER JOIN {{ ref('order_details') }} d ON o.dw_country_code = d.dw_country_code AND o.id = d.order_id
  INNER JOIN {{ ref('catalog') }} c ON d.dw_country_code = c.dw_country_code AND d.product_id = c.product_id
  WHERE d.special_type = 'GWS' and status=1
  GROUP BY sol.dw_country_code,
           sol.sub_id
),
box_global_grades AS (
SELECT p.dw_country_code, p.box_id, p.coffret_id,  max(global_grade) AS global_grade
FROM `teamdata-291012.Spreadsheet_synchro.raw_doc_compo` c
JOIN {{ ref('products') }} p ON p.sku = c.sku_compo
GROUP BY p.dw_country_code, p.box_id, p.coffret_id
)
SELECT FT.*,
ROW_NUMBER() OVER(PARTITION BY user_id, sequence_group ORDER BY box_id) AS consecutive_boxes from (
select full_table.*,
    SUM(is_new_sequence) OVER(PARTITION BY user_id ORDER BY box_id ROWS UNBOUNDED PRECEDING) AS sequence_group

 from (
SELECT concat(t.dw_country_code,'_',t.user_id)as user_key,t.*,
case when cm.mono_box_id is null then false else true end as is_mono,
cm.mono_brand as mono_brand,
t.box_id+1 as next_month_id,
lag(t.date) over (partition by t.user_id,t.dw_country_code order by t.box_id) last_box_received_date,
case when  lead(t.box_id) over (partition by t.user_id,t.dw_country_code order by t.box_id) - t.box_id IN (0,1) -- next box by user is the next box
OR lead(t.box_id) over (partition by t.order_detail_id,t.dw_country_code order by t.box_id) - t.box_id = 1  -- next box in the subscription (by order_detail)
then 'LIVE' else'CHURN'end as next_month_status,
case when (lead(t.box_id) over (partition by t.user_id,t.dw_country_code order by t.box_id) - t.box_id IN (0,1) -- next box by user is the next box
OR lead(t.box_id) over (partition by t.order_detail_id,t.dw_country_code order by t.box_id) - t.box_id = 1) 
AND cannot_suspend =1 
  -- next box in the subscription (by order_detail)
then 1 else 0 end as next_month_committment,
cASE
      WHEN LAG(t.box_id) OVER(PARTITION BY t.user_id ORDER BY t.box_id) IS NULL THEN 1  -- Première box
      WHEN t.box_id - LAG(t.box_id) OVER(PARTITION BY t.user_id ORDER BY t.box_id) > 1 THEN 1  -- Trou détecté
      ELSE 0  -- Box consécutive
    END AS is_new_sequence,
    -- Compte cumulatif des box pour l'utilisateur
    ROW_NUMBER() OVER(PARTITION BY t.user_id ORDER BY t.box_id) AS total_boxes_so_far,

CASE 
  WHEN lead(t.box_id) over (partition by t.user_id,t.dw_country_code order by t.box_id) - t.box_id IN (0,1) OR lead(t.box_id) over (partition by t.order_detail_id,t.dw_country_code order by t.box_id) - t.box_id = 1 THEN NULL
  WHEN t.gift = 1  THEN 'gift end'
  ELSE COALESCE(scr.sub_suspended_reason_lvl1, 'self-willed')
   END
AS sub_suspended_reason_lvl1,
CASE 
  WHEN lead(t.box_id) over (partition by t.user_id,t.dw_country_code order by t.box_id) - t.box_id IN (0,1) OR lead(t.box_id) over (partition by t.order_detail_id,t.dw_country_code order by t.box_id) - t.box_id = 1 THEN NULL
  WHEN t.gift = 1  THEN 'gift end'
  ELSE COALESCE(scr.sub_suspended_reason_lvl2, 'suspended')
  END
AS sub_suspended_reason_lvl2,
CASE 
  WHEN lead(t.box_id) over (partition by t.user_id,t.dw_country_code order by t.box_id) - t.box_id IN (0,1) OR lead(t.box_id) over (partition by t.order_detail_id,t.dw_country_code order by t.box_id) - t.box_id = 1 THEN NULL
  WHEN t.gift = 1  THEN 'gift end'
  ELSE COALESCE(scr.sub_suspended_reason_lvl3, 'suspended')
  END
AS sub_suspended_reason_lvl3,
bgg.global_grade AS box_global_grade,


mcd.type as coupon_type,   -- new coupon_typet.box_id
case when t.box_id - lag(t.box_id) over (partition by t.user_id, t.dw_country_code order by t.box_id, t.order_detail_id )  IN (0,1) 
OR
t.box_id - lag(t.box_id) over (partition by t.order_detail_id, t.dw_country_code order by t.box_id, t.order_detail_id )  = 1
then 'LIVE'
else'ACQUISITION'end as acquis_status_lvl1,
case when  t.box_id - lag(t.box_id) over (partition by t.user_id, t.dw_country_code order by t.box_id, t.order_detail_id) IN (0,1)
OR
t.box_id - lag(t.box_id) over (partition by t.order_detail_id, t.dw_country_code order by t.box_id, t.order_detail_id )  = 1
then 'LIVE'  
when t.gift=1 then 'GIFT'
when lag(t.box_id) over (partition by t.user_id,t.dw_country_code order by t.box_id) is not null then 'REACTIVATION'
when lag(t.box_id) over (partition by t.user_id,t.dw_country_code order by t.box_id) is null then 'NEW NEW'
else'Unknown'end as acquis_status_lvl2,   
CASE WHEN cannot_suspend = 1 THEN 'committed' ELSE 'not committed' END AS committed,
g.gws_costs,
t.total_product / (1+vat_rate/100) AS gross_revenue,
t.total_product - t.total_product/(1+vat_rate/100) AS vat_on_gross_revenue,
t.total_discount / (1+vat_rate/100) AS discount,
t.total_discount - t.total_discount / (1+vat_rate/100) AS vat_on_discount,
t.total_product / (1+vat_rate/100) - (t.total_discount / (1+vat_rate/100)) AS net_revenue,
t.total_shipping/(1+vat_rate/100) AS shipping,
t.total_shipping - t.total_shipping/(1+vat_rate/100) AS vat_on_shipping,
t.total_product / (1+vat_rate/100) - (t.total_discount / (1+vat_rate/100)) - (t.coop + t.assembly_cost + t.pack_cost + t.print_cost + t.consumable_cost + t.shipping_cost + COALESCE(g.gws_costs, 0)) AS gross_profit
FROM
(
  SELECT 
  o.dw_country_code,
  s.id AS sub_id,
  d.id AS order_detail_id,
  o.id AS order_id,
  o.user_id, 
  s.box_id,
  case when s.box_id=current_box_id then true else false end as is_current,
  b.id - cbt.current_box_id as diff_current_box,
  s.coffret_id,
  b.date,
   DATE_DIFF(CASE WHEN an.eventDate IS NULL THEN date(s.last_payment_date) ELSE date(an.eventDate) END, date(b.shipping_date), DAY) + 1  AS day_in_cycle,
  CASE WHEN an.eventDate IS NULL THEN date(s.last_payment_date) ELSE date(an.eventDate) END AS payment_date,
 DATE_DIFF(CASE WHEN an.eventDate IS NULL THEN date(s.last_payment_date) ELSE date(an.eventDate) END, date(b.shipping_date), DAY) + 1 AS nb_days_since_opening,
 date_diff(CASE WHEN an.eventDate IS NULL THEN date(s.last_payment_date) ELSE date(an.eventDate) END, date(b1.shipping_date), DAY) as nb_days_next_cycle,
  extract(month FROM b.date) AS month,
  extract(year FROM b.date) AS year,
  o.coupon_code_id,
  COALESCE(coupons_parents.code, c.code) AS coupon_code,
  s.sub_offer_id,
  COALESCE(so_parents.code, so.code) AS sub_offer_code,
  CASE WHEN s.box_id = d.sub_start_box THEN  COALESCE(coupons_parents.code, c.code) ELSE COALESCE(so_parents.code, so.code) END AS coupon,
  s.reactivated_date,
  s.shipping_mode,
  CASE WHEN d.gift_card_id = 0 THEN 1
       WHEN bg.gift IS NULL THEN 1
       WHEN bg.gift = 1 THEN 0
       WHEN bg.gift = 0 THEN 1
       ELSE 1
  END AS self,
  CASE WHEN bg.gift IS NULL THEN 0 
       WHEN bg.gift = 1 THEN 1
       WHEN bg.gift = 0 THEN 0
       ELSE bg.gift
  END AS gift,
  CASE WHEN yc.yearly_coupon_id IS NOT NULL AND s.cannot_suspend = 1 THEN 1 ELSE 0 END AS yearly,
  CASE WHEN d.quantity= -12 THEN 1 ELSE 0 END AS old_yearly,
  d.quantity AS dquantity,
  s.cannot_suspend AS cannot_suspend,
  CASE  WHEN s.sub_payment_status_id = 8 OR o.status_id = 3 THEN 0
        WHEN s.total_product = 0 AND gc.id IS NULL THEN 0
        WHEN s.total_product = 0 AND gc.id IS NOT NULL AND pbp.sub_id IS NULL THEN b.box_quantity*(gc.amount/gc.duration)
        WHEN s.total_product = 0 AND gc.id IS NOT NULL AND pbp.sub_id IS NOT NULL THEN (gc.amount/gc.duration) -- if partial box paid, count only one box
        ELSE s.total_product 
  END AS total_product,
  o.dw_country_code AS store_code,
  COALESCE(tva.taux, 0) AS vat_rate,
  CASE WHEN s.sub_payment_status_id = 8 OR o.status_id = 3 THEN 0.0
       WHEN c.parent_id = 15237671 AND s.box_id = d.sub_start_box THEN 0.0 -- Veepee offer - May 2021
       WHEN so.parent_offer_id = 53382 THEN 0.0 -- Veepee offer - May 2021
       ELSE s.total_discount
  END AS total_discount,
  s.shipping_country AS shipping_country,
  CASE WHEN s.sub_payment_status_id = 8 OR o.status_id = 3 THEN 0.0 ELSE s.total_shipping END AS total_shipping,
  CASE WHEN s.sub_payment_status_id = 3 THEN 'forthcoming' ELSE 'paid' END AS payment_status,
  sps.name AS sub_payment_status,
  sub_payment_status_id,
  
d.sub_start_box,
CASE WHEN o.raf_parent_id > 0 and rol.order_id is not null THEN 1 ELSE 0 END AS raffed,
raf_parent_id,
  s.shipping_firstname,
  s.shipping_lastname,
  d.gift_card_id,
  CASE  WHEN (c.discount_type = 'PRODUCT' AND d.sub_start_box = s.box_id) OR so.offer_type = 'PRODUCT' THEN 'GWS'
        WHEN (c.discount_type IN ('CURRENCY', 'PERCENT', 'CURRENCY_TOTAL') AND d.sub_start_box = s.box_id) OR so.offer_type IN ('CURRENCY','PERCENT','CURRENCY_TOTAL') THEN 'discount'
        ELSE 'Other'
  END AS discount_type,
  CASE WHEN (c.sub_engagement_period > 0 AND d.sub_start_box = s.box_id) OR so.sub_engagement_period > 0 THEN 'engaged' ELSE 'not engaged' END AS coupon_engagement,
  kc.coop,
  kc.assembly_cost,
  kc.pack_cost,
  kc.print_cost,
  kc.consumable_cost,
  sc.price AS shipping_cost,
  b1.date as next_month_date,
  s.next_payment_date,
  s.last_payment_date,
  CASE 
  WHEN  s.cannot_suspend = 1 
        AND (
            lead(s.cannot_suspend) over (partition by s.order_detail_id,s.dw_country_code order by s.box_id) = 0 
            OR lead(s.cannot_suspend) over (partition by s.order_detail_id,s.dw_country_code order by s.box_id) IS NULL
            )

    THEN 1 
    ELSE 0 END AS last_committed_box
  -- sub_suspended_reason_lvl1,sub_suspended_reason_lvl2,sub_suspended_reason_lvl3
  FROM {{ ref('orders') }} o
  INNER JOIN {{ ref('order_details') }} d ON o.id = d.order_id AND o.dw_country_code = d.dw_country_code
  INNER JOIN {{ ref('order_detail_sub') }} s ON s.order_detail_id = d.id AND s.dw_country_code = d.dw_country_code
  INNER JOIN {{ ref('boxes') }} b ON b.id = s.box_id AND b.dw_country_code = s.dw_country_code
  INNER JOIN {{ ref('boxes') }} b1 ON b1.id = s.box_id +1 AND b1.dw_country_code = s.dw_country_code
  INNER JOIN {{ ref('sub_payments_status') }} sps ON sps.id = s.sub_payment_status_id and sps.dw_country_code='FR'
  INNER JOIN {{ ref('current_box') }} cbt ON o.dw_country_code = cbt.dw_country_code
  LEFT JOIN products p ON o.dw_country_code = p.dw_country_code AND b.id = p.box_id AND s.coffret_id = p.coffret_id AND p.product_codification_id = 29
  LEFT JOIN {{ ref('kit_costs') }} kc ON o.dw_country_code = kc.country_code AND p.inventory_item_id = kc.inventory_item_id and kc.kit_id=p.id
  LEFT JOIN shipping_mode_dedup  sc ON b.date >= sc.date_start AND (b.date <= sc.date_end OR sc.date_end IS NULL) AND s.shipping_mode = sc.shipping_mode_id AND CASE WHEN b.box_quantity = 1 THEN 0.4 WHEN b.box_quantity = 2 THEN 0.8 END >= min_weight AND (CASE WHEN b.box_quantity = 1 THEN 0.4 WHEN b.box_quantity = 2 THEN 0.8 END < max_weight OR max_weight IS NULL)
  LEFT JOIN {{ ref('gift_cards') }} gc ON gc.ID = d.gift_card_id AND gc.dw_country_code = d.dw_country_code
  LEFT JOIN {{ ref('adyen_notifications_authorization') }} an ON an.sub_id = s.id AND an.dw_country_code = s.dw_country_code
  LEFT JOIN {{ ref('coupons') }} c ON c.id = o.coupon_code_id AND c.dw_country_code = o.dw_country_code
  LEFT JOIN {{ ref('coupons') }} coupons_parents ON coupons_parents.id = c.parent_id AND coupons_parents.dw_country_code = c.dw_country_code
  LEFT JOIN {{ ref('sub_offers') }} so ON so.id = s.sub_offer_id AND so.dw_country_code = s.dw_country_code
  LEFT JOIN {{ ref('sub_offers') }} so_parents ON so_parents.id = so.parent_offer_id AND so_parents.dw_country_code = so.dw_country_code
  LEFT JOIN inter.tva_product tva ON tva.country_code = s.shipping_country AND tva.category = 'normal' AND tva.dw_country_code = s.dw_country_code
  LEFT JOIN {{ ref('box_gift') }} bg ON bg.dw_country_code = s.dw_country_code AND bg.sub_id = s.id
  LEFT JOIN snippets.yearly_coupons yc ON o.dw_country_code = yc.country_code AND o.coupon_code_id = yc.yearly_coupon_id
  LEFT JOIN {{ ref('raf_order_link') }} rol on o.id=rol.order_id and rol.dw_country_code=d.dw_country_code
  /*LEFT JOIN (select user_id,month,year,dw_country_code,box_id,max(sub_suspended_reason_lvl1)sub_suspended_reason_lvl1,max(sub_suspended_reason_lvl2)sub_suspended_reason_lvl2,max(sub_suspended_reason_lvl3)sub_suspended_reason_lvl3 from`teamdata-291012.sales.box_sales_by_user_by_type`  group by 1,2,3,4,5)bsbu ON o.dw_country_code = bsbu.dw_country_code AND bsbu.user_id=o.user_id and bsbu.box_id = s.box_id + 1*/
  LEFT JOIN {{ ref('partial_box_paid') }} pbp ON pbp.dw_country_code = s.dw_country_code AND pbp.sub_id = s.id
  WHERE -- o.status_id IN (1, 3) AND 
  (s.shipping_status_id IN (2, 3, 4, 5, 19, 22) OR (s.sub_payment_status_id = 3 AND s.box_id >= cbt.current_box_id)
  -------------------
  OR (o.dw_country_code = 'DE' and s.sub_payment_status_id = 3 and s.box_id=160)
  -- cas particuler allemagne avril 2025
  )
  AND s.box_id <= cbt.current_box_id + 36
  

) t
LEFT JOIN gws_costs_table g USING(dw_country_code, sub_id)
LEFT JOIN ( select distinct country,code,max(type) as type, max(date) as date,max(type2) as type2,max(coupon_id) as coupon_id,max(sub_offer_id) as sub_offer_id 
from`teamdata-291012.marketing.Marketing_cac_discount` 
group by 1,2) 
mcd on mcd.coupon_id=coupon_code_id and mcd.country=t.dw_country_code
LEFT JOIN 
(  select distinct country,code,max(type) as type, max(date) as date,max(type2) as type2,max(coupon_id) as coupon_id,max(sub_offer_id) as sub_offer_id 
from`teamdata-291012.marketing.Marketing_cac_discount` 
group by 1,2) mcdso on mcdso.sub_offer_id=t.sub_offer_id and mcd.country=t.dw_country_code
LEFT JOIN box_global_grades bgg ON bgg.dw_country_code = t.dw_country_code AND bgg.box_id = t.box_id AND bgg.coffret_id = t.coffret_id
LEFT JOIN self_churn_reason scr ON scr.dw_country_code = t.dw_country_code AND scr.user_id = t.user_id AND scr.box_id = t.box_id+1
LEFT JOIN {{ ref('box_mono') }}  cm on t.dw_country_code=cm.dw_country_code and t.box_id=cm.mono_box_id and t.coffret_id=cm.mono_coffret_id) full_table

group by all) FT

group by all
