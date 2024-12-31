WITH all_boxes_by_user AS (
  SELECT bs.dw_country_code, bs.user_id, bs.box_id
  FROM {{ ref('box_sales') }} AS bs
)
SELECT t.dw_country_code,
       t.box_id,
       MAX(b.shipping_date) AS cycle_start_date,
       FORMAT_DATE('%Y-%m', MAX(b.date)) AS m,
       DATE_DIFF(MAX(last_payment_date), MAX(b.shipping_date), DAY) + 1 AS day_in_cycle,
       CASE WHEN last_payment_date >= b.shipping_date THEN t.last_payment_date END AS d,
       CASE WHEN last_payment_date < b.shipping_date THEN 'before opening cycle' ELSE 'after opening cycle' END AS cycle,
       CASE WHEN self = 0 THEN 'gift'
            WHEN fb.first_box_id = t.box_id THEN 'new'
            ELSE 'reactivation'
       END AS type,
       COUNT(*) AS nb
FROM
(
    SELECT o.dw_country_code,
           s.box_id,
           s.last_payment_date,
           o.user_id,
           CASE WHEN d.gift_card_id = 0 OR (d.gift_card_id > 0 AND (s.box_id - d.sub_start_box >= d.quantity)) THEN 1 ELSE 0 END AS self
           FROM {{ ref('orders') }} o
           INNER JOIN {{ ref('order_details') }} d ON o.id = d.order_id AND o.dw_country_code = d.dw_country_code
           INNER JOIN {{ ref('order_detail_sub') }} s ON s.order_detail_id = d.id AND s.dw_country_code = d.dw_country_code
           LEFT JOIN all_boxes_by_user bu ON o.dw_country_code = bu.dw_country_code AND bu.box_id + 1 = s.box_id AND o.user_id = bu.user_id
           WHERE o.status_id IN (1, 3)
           AND s.shipping_Status_id IN (2, 3, 4, 5, 22)
           AND bu.box_id IS NULL
) t
INNER JOIN inter.boxes b ON b.id = t.box_id AND t.dw_country_code = b.dw_country_code
INNER JOIN (SELECT dw_country_code, user_id, MIN(box_id) AS first_box_id
            FROM all_boxes_by_user
            GROUP BY dw_country_code,
                     user_id) fb ON t.dw_country_code = fb.dw_country_code AND t.user_id = fb.user_id
GROUP BY t.dw_country_code, t.box_id, d, cycle, type
