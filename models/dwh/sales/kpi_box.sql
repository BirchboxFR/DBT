WITH boxes_total AS
(
  SELECT dw_country_code, year, month, count(*) AS value
  FROM {{ ref('box_sales') }} as bs
  WHERE 1=1
  GROUP BY dw_country_code, year, month
),
boxes_shipped AS 
(
  SELECT dw_country_code, year, month, count(*) AS value
  FROM {{ ref('box_sales') }} as bs
  WHERE 1=1
  AND bs.payment_status = 'paid'
  GROUP BY dw_country_code, year, month
),
boxes_forthcoming AS
(
  SELECT dw_country_code, year, month, count(*) AS value
  FROM {{ ref('box_sales') }} as bs
  WHERE 1=1
  AND bs.payment_status = 'forthcoming'
  GROUP BY dw_country_code, year, month
),
boxes_free AS
(
  SELECT dw_country_code, year, month, count(*) AS value
  FROM {{ ref('box_sales') }} as bs
  WHERE 1=1
  AND bs.payment_status = 'paid'
  AND bs.sub_payment_status_id = 8
  GROUP BY dw_country_code, year, month
),
boxes_reexp AS
(
  SELECT bs.dw_country_code, bs.year, bs.month, count(distinct bs.sub_id) AS value
  FROM {{ ref('tags') }} ta
  JOIN {{ ref('box_sales') }} bs ON bs.sub_id = ta.link_id AND ta.type = 'SUB' AND ta.dw_country_code = bs.dw_country_code
  WHERE 1=1
  GROUP BY bs.dw_country_code, bs.year, bs.month
),
all_subs_for_churn AS 
(

  SELECT bsbu.dw_country_code, bsbu.box_month,
  SUM(bsbu.m_nb) nb_subs,
  SUM(bsbu.m_1_nb) nb_subs_lm
  FROM {{ ref('box_sales_by_user_by_type') }} bsbu
--   WHERE bsbu.type NOT LIKE 'GIFT%'
  GROUP BY bsbu.dw_country_code, bsbu.box_month

),
churn AS
(
  
SELECT bsbu.dw_country_code, bsbu.box_month , -- bsbu.sub_suspended_reason_lvl1,
SUM(CASE WHEN bsbu.diff < 0 THEN -bsbu.diff ELSE 0 END) AS nb_churn,
max(subs.nb_subs_lm)  AS nb_subs_last_month,
SAFE_DIVIDE(SUM(CASE WHEN bsbu.diff < 0 THEN -bsbu.diff ELSE 0 END),max(subs.nb_subs_lm)) AS total_churn
FROM {{ ref('box_sales_by_user_by_type') }} bsbu
JOIN all_subs_for_churn AS subs ON subs.dw_country_code = bsbu.dw_country_code AND bsbu.box_month = subs.box_month
WHERE 1=1
AND bsbu.diff < 0
GROUP BY bsbu.dw_country_code, bsbu.box_month-- , bsbu.sub_suspended_reason_lvl1

),
all_subs_for_self_churn AS 
(

  SELECT bsbu.dw_country_code, bsbu.box_month, 
  CASE WHEN bsbu.type LIKE 'GIFT%' THEN 'gift' ELSE 'self' END As sub_type,
  SUM(bsbu.m_nb) nb_subs,
  SUM(bsbu.m_1_nb) nb_subs_lm
  FROM {{ ref('box_sales_by_user_by_type') }} bsbu
   
  GROUP BY bsbu.dw_country_code, bsbu.box_month, sub_type

),
self_churn AS
(
 SELECT bsbu.dw_country_code, bsbu.box_month, bsbu.sub_suspended_reason_lvl1,
SUM(CASE WHEN bsbu.diff < 0 THEN -bsbu.diff ELSE 0 END) AS nb_churn,
max(subs.nb_subs_lm)  AS nb_subs_last_month,
SAFE_DIVIDE(SUM(CASE WHEN bsbu.diff < 0 THEN -bsbu.diff ELSE 0 END),max(subs.nb_subs_lm)) AS total_churn
FROM {{ ref('box_sales_by_user_by_type') }} bsbu
JOIN all_subs_for_self_churn AS subs ON subs.dw_country_code = bsbu.dw_country_code AND bsbu.box_month = subs.box_month AND subs.sub_type = 'self'
WHERE 1=1
AND bsbu.diff < 0
AND bsbu.type NOT LIKE 'GIFT%'
GROUP BY bsbu.dw_country_code, bsbu.box_month , bsbu.sub_suspended_reason_lvl1

),

gift_churn AS
(
 SELECT bsbu.dw_country_code, bsbu.box_month, bsbu.sub_suspended_reason_lvl1,
SUM(CASE WHEN bsbu.diff < 0 THEN -bsbu.diff ELSE 0 END) AS nb_churn,
max(subs.nb_subs_lm)  AS nb_subs_last_month,
SAFE_DIVIDE(SUM(CASE WHEN bsbu.diff < 0 THEN -bsbu.diff ELSE 0 END),max(subs.nb_subs_lm)) AS total_churn
FROM {{ ref('box_sales_by_user_by_type') }} bsbu
JOIN all_subs_for_self_churn AS subs ON subs.dw_country_code = bsbu.dw_country_code AND bsbu.box_month = subs.box_month AND subs.sub_type <> 'self'
WHERE 1=1
AND bsbu.diff < 0
AND bsbu.type LIKE 'GIFT%'
GROUP BY bsbu.dw_country_code, bsbu.box_month , bsbu.sub_suspended_reason_lvl1

),

acquis AS 
(
  SELECT dw_country_code, year, month, 
  SUM(CASE WHEN gift = 1 THEN diff ELSE 0 END) AS gift,
  SUM(CASE WHEN self = 1 AND new_sub = 1 THEN diff ELSE 0 END) AS new_new,
  SUM(CASE WHEN self = 1 AND new_sub = 0 THEN diff ELSE 0 END) AS reactivation
  FROM {{ ref('box_sales_by_user_by_type') }}
  WHERE diff > 0
  GROUP BY dw_country_code, year, month
),

all_boxes as 
(
  SELECT b.dw_country_code, id as box_id, extract(year from b.date) as year, extract(month from b.date) AS month, format_date('%Y-%m', b.date) AS box_month, CASE WHEN b.shipping_status_id = 2 THEN true ELSE false END AS is_current, 
  b.id - cb.current_box_id AS diff_current_box
  FROM {{ ref('boxes') }} b
  JOIN {{ ref('current_box') }} cb ON cb.dw_country_code = b.dw_country_code
  ORDER BY b.dw_country_code, b.date
)


SELECT all_boxes.dw_country_code, all_boxes.year, all_boxes.month, is_current, all_boxes.box_month, all_boxes.box_id,all_boxes.diff_current_box,
boxes_total.value AS box_total,
boxes_shipped.value AS box_shipped,
boxes_forthcoming.value AS box_forthcoming,
boxes_free.value AS box_free,
boxes_reexp.value AS box_reexp,

churn.nb_churn AS churn_nb_total,
churn.nb_subs_last_month AS total_last_box,
churn.total_churn AS churn_rate_total,
self_suspended.nb_churn AS churn_self_willed_nb,
self_suspended.nb_subs_last_month AS nb_self_subs_last_month,
self_suspended.total_churn AS churn_rate_self_willed,
SUM(self_tech.nb_churn) AS churn_self_tech_nb,
SUM(self_tech.total_churn) AS churn_rate_self_tech,
gift_churn.nb_churn AS churn_gift_nb,
gift_churn.nb_subs_last_month AS nb_gift_subs_last_month,
gift_churn.total_churn AS churn_rate_gift,
acquis.new_new + acquis.reactivation + acquis.gift AS acquis_total,
acquis.new_new AS acquis_new_new,
acquis.reactivation AS acquis_reactivation,
acquis.gift AS acquis_gift



FROM all_boxes
LEFT JOIN boxes_total ON all_boxes.dw_country_code = boxes_total.dw_country_code AND all_boxes.year = boxes_total.year AND all_boxes.month = boxes_total.month
LEFT JOIN boxes_shipped ON all_boxes.dw_country_code = boxes_shipped.dw_country_code AND all_boxes.year = boxes_shipped.year AND all_boxes.month = boxes_shipped.month
LEFT JOIN boxes_forthcoming ON all_boxes.dw_country_code = boxes_forthcoming.dw_country_code AND all_boxes.year = boxes_forthcoming.year AND all_boxes.month = boxes_forthcoming.month
LEFT JOIN boxes_free ON all_boxes.dw_country_code = boxes_free.dw_country_code AND all_boxes.year = boxes_free.year AND all_boxes.month = boxes_free.month
LEFT JOIN boxes_reexp ON all_boxes.dw_country_code = boxes_reexp.dw_country_code AND all_boxes.year = boxes_reexp.year AND all_boxes.month = boxes_reexp.month
LEFT JOIN churn ON all_boxes.dw_country_code = churn.dw_country_code AND all_boxes.box_month = churn.box_month
LEFT JOIN self_churn self_suspended ON all_boxes.dw_country_code = self_suspended.dw_country_code AND all_boxes.box_month = self_suspended.box_month AND self_suspended.sub_suspended_reason_lvl1 = 'self-willed'
LEFT JOIN self_churn self_tech ON all_boxes.dw_country_code = self_tech.dw_country_code AND all_boxes.box_month = self_tech.box_month AND  self_tech.sub_suspended_reason_lvl1 = 'technical'
LEFT JOIN gift_churn  ON all_boxes.dw_country_code = gift_churn.dw_country_code AND all_boxes.box_month = gift_churn.box_month 
LEFT JOIN acquis ON acquis.dw_country_code = all_boxes.dw_country_code AND acquis.year = all_boxes.year AND acquis.month = all_boxes.month

GROUP BY 
all_boxes.dw_country_code, all_boxes.year, all_boxes.month, is_current, all_boxes.box_month, all_boxes.box_id, all_boxes.diff_current_box,
box_total,
box_shipped,
box_forthcoming,
box_free,
box_reexp,
churn.nb_churn,
churn.nb_subs_last_month,
churn.total_churn,
self_suspended.nb_churn,
self_suspended.nb_subs_last_month,
self_suspended.total_churn,
churn_gift_nb,
nb_gift_subs_last_month,
churn_rate_gift,
acquis.new_new,
acquis.reactivation,
acquis.gift
ORDER BY all_boxes.dw_country_code, all_boxes.year, all_boxes.month