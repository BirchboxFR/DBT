SELECT
  bs.dw_country_code AS dw_country_code ,
  bs.box_id,
  b.date,
  bs.year,  
  bs.month,
  COALESCE(ch.choice_name, 'no choice') AS choice,
 bs.user_id, bs.sub_id,
 -- STATUT SUB
 bs.acquis_status_lvl1,
  bs.acquis_status_lvl2,
   -- INFOS CHOOSE
   cf.name as form_name,
   ch.choice_date,
  CASE WHEN MAX(ch.user_id) IS NOT NULL THEN 1 ELSE 0 END AS choose,
  CASE WHEN DATE_DIFF(ch.choice_date,b.NME_start_date,DAY) = 0 THEN 1 ELSE NULL END as choose_live_1st_day,
  CASE WHEN DATE_DIFF(ch.choice_date,b.NME_start_date,DAY) = 1 THEN 1 ELSE NULL END as choose_live_2nd_day,
  CASE WHEN DATE_DIFF(ch.choice_date,b.NME_start_date,DAY) = 2 THEN 1 ELSE NULL END as choose_live_3rd_day,
  CASE WHEN DATE_DIFF(ch.choice_date,b.NME_start_date,DAY) = 3 THEN 1 ELSE NULL END as choose_live_4th_day,
  b.nme_start_date,
b.nme_end_date
  FROM {{ ref('box_sales') }} as bs
 -- JOIN inter.orders o ON o.id = bs.order_id AND o.dw_country_code = bs.dw_country_code
 -- JOIN inter.order_detail_sub s ON s.id = bs.sub_id AND s.dw_country_code = bs.dw_country_code
  JOIN inter.boxes b ON b.id = bs.box_id AND b.dw_country_code = bs.dw_country_code
  LEFT JOIN  inter.choose_forms cf ON cf.box_id = b.id and cf.dw_country_code = b.dw_country_code
  LEFT JOIN
  (
    SELECT user_id, box_id, cc.choice_name, cu.dw_country_code, cu.status_id, cu.created_at as choice_date, cf.name as form_name
    FROM inter.choose_users cu
    JOIN inter.choose_forms cf ON cf.id = cu.form_id AND cf.dw_country_code = cu.dw_country_code
    JOIN inter.choose_choices cc ON cc.id = cu.choice_id AND cc.dw_country_code = cu.dw_country_code

    GROUP BY user_id, box_id, cc.choice_name, cu.dw_country_code, cu.status_id, cu.created_at, cf.name
  ) ch ON ch.user_id = bs.user_id AND ch.box_id = bs.box_id AND ch.dw_country_code = bs.dw_country_code AND ch.status_id IN ( 1,2)
  -- WHERE bs.year = 2024 AND bs.month =6 AND bs.dw_country_code="FR"
  GROUP BY bs.box_id, bs.year, bs.month,b.date, ch.choice_name, bs.dw_country_code, bs.user_id, ch.choice_date,  b.nme_start_date, b.nme_end_date,cf.name,  bs.acquis_status_lvl1, bs.sub_id,
  bs.acquis_status_lvl2
