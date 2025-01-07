WITH ranked_sub_history AS
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
  JOIN `inter.sub_suspended_reasons` ssr ON ssr.dw_country_code = sh.dw_country_code AND ssr.id = sh.sub_suspended_reasons_id
  
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
  FROM {{ ref('adyen_notifications') }} an
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
  SELECT dw_country_code, user_id, box_id, sub_suspended_reason_lvl1, sub_suspended_reason_lvl2, sub_suspended_reason_lvl3,
  ROW_NUMBER() OVER (PARTITION BY user_id, box_id, dw_country_code ORDER BY d DESC) AS row_num
  FROM all_reasons
),
self_churn_reason AS
(
  SELECT * EXCEPT(row_num)
  FROM all_reasons_ranked
  WHERE row_num = 1
)


SELECT t.*, 
CASE 
  WHEN diff < 0 AND type LIKE 'GIFT%'  THEN 'gift end'
  WHEN diff < 0 THEN COALESCE(scr.sub_suspended_reason_lvl1, 'self-willed')
  ELSE NULL END
AS sub_suspended_reason_lvl1,
CASE 
  WHEN diff < 0 AND type LIKE 'GIFT%'  THEN 'gift end'
  WHEN diff < 0 THEN COALESCE(scr.sub_suspended_reason_lvl2, 'suspended')
  ELSE NULL END
AS sub_suspended_reason_lvl2,
CASE 
  WHEN diff < 0 AND type LIKE 'GIFT%'  THEN type
  WHEN diff < 0 THEN COALESCE(scr.sub_suspended_reason_lvl3, 'suspended')
  ELSE NULL END
AS sub_suspended_reason_lvl3

FROM
(
  SELECT COALESCE(m.dw_country_code, m_1.dw_country_code) AS dw_country_code,
        COALESCE(m.box_id, m_1.box_id + 1) AS box_id,
        COALESCE(m.year, m_1.year) AS year,
        COALESCE(m.month, m_1.month) AS month,
        COALESCE(m.box_month, m_1.box_month) AS box_month,
        COALESCE(m.user_id, m_1.user_id) AS user_id,
        COALESCE(m.self, m_1.self) AS self,
        COALESCE(m.gift, m_1.gift) AS gift,
        COALESCE(m.type, m_1.type) as type,
        COALESCE(m.nb, 0) as m_nb,
        COALESCE(m_1.nb, 0) as m_1_nb,
        COALESCE(m.new_sub, 0) AS new_sub,
        COALESCE(m.nb, 0) - COALESCE(m_1.nb, 0) AS diff,
        payment_date
  FROM
  (
    SELECT bs.dw_country_code,
          bs.box_id,
          bs.year,
          bs.month,
          bs.user_id,
          FORMAT_DATE('%Y-%m', bs.date)  as box_month,
          bs.self,
          bs.gift,
          CASE WHEN bs.yearly = 1 OR bs.old_yearly = 1 THEN 'yearly'
                WHEN gift = 1 THEN 'GIFT ' || bs.dquantity
                ELSE 'self'
          END AS type,
          CASE WHEN bs.box_id = first_box.first_box THEN 1
                ELSE 0
          END AS new_sub,
          MAX(bs.payment_date) AS payment_date,
          COUNT(*) AS nb
    FROM {{ ref('box_sales') }} bs
    LEFT JOIN
    (
        SELECT dw_country_code,
              user_id,
              MIN(box_id) AS first_box
        FROM {{ ref('box_sales') }} bs
        GROUP BY dw_country_code,
                user_id
    ) first_box ON first_box.dw_country_code = bs.dw_country_code AND first_box.user_id = bs.user_id
    GROUP BY bs.dw_country_code,
            bs.box_id,
            bs.year,
            bs.month,
            bs.user_id,
            bs.self,
            bs.gift,
            type,
            new_sub,
            bs.date
  ) m

  FULL OUTER JOIN 

  (
    SELECT bs.dw_country_code,
          bs.box_id,
          extract(year from b.date) as year,
          extract(month from b.date) AS month,
          FORMAT_DATE('%Y-%m', b.date)  as box_month,
          bs.user_id,
          bs.self,
          bs.gift,   
          CASE WHEN bs.yearly = 1 OR bs.old_yearly = 1 THEN 'yearly'
              WHEN gift = 1 THEN 'GIFT ' || bs.dquantity
              ELSE 'self'
          END AS type,
          COUNT(*) as nb
    FROM {{ ref('box_sales') }} bs
    JOIN inter.boxes b ON b.id = bs.box_id + 1 AND b.dw_country_code = bs.dw_country_code
    GROUP BY bs.dw_country_code,
            bs.box_id,
            bs.year,
            bs.month,
            bs.user_id,
            bs.self,
            bs.gift,
            type,
            b.date
  ) m_1 ON m_1.dw_country_code = m.dw_country_code AND m_1.user_id = m.user_id AND m.box_id - 1 = m_1.box_id
) t
LEFT JOIN self_churn_reason scr ON scr.dw_country_code = t.dw_country_code AND scr.user_id = t.user_id AND scr.box_id = t.box_id

