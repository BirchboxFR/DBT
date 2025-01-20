SELECT o.dw_country_code, o.id AS order_id, s.id AS sub_id, d.quantity, s.box_id, SUM(b1.box_quantity) AS box_received,
CASE 
  WHEN SUM(b1.box_quantity) <= d.quantity THEN 1 
  WHEN SUM(b1.box_quantity) > d.quantity AND SUM(b1.box_quantity) - b.box_quantity < d.quantity THEN 1
ELSE 0 END AS gift
FROM {{ ref('orders') }} o
INNER JOIN {{ ref('order_details') }} d ON o.dw_country_code = d.dw_country_code AND o.id = d.order_id
INNER JOIN {{ ref('order_detail_sub') }} s ON s.dw_country_code = d.dw_country_code AND s.order_detail_id = d.id
INNER JOIN {{ ref('order_detail_sub') }} s1 ON s1.dw_country_code = s.dw_country_code AND s1.order_detail_id = s.order_detail_id AND s1.box_id <= s.box_id
INNER JOIN {{ ref('boxes') }} b ON b.dw_country_code = s.dw_country_code AND b.id = s.box_id
INNER JOIN {{ ref('boxes') }} b1 ON b1.dw_country_code = s1.dw_country_code AND b1.id = s1.box_id
INNER JOIN {{ ref('current_box') }} cb ON o.dw_country_code = cb.dw_country_code
WHERE o.status_id IN (1, 3)
AND (s.shipping_status_id IN (2, 3, 4, 5, 19, 22) OR (s.sub_payment_status_id = 3 AND s.box_id >= cb.current_box_id))
AND d.gift_card_id > 0
GROUP BY o.dw_country_code, o.id, s.id, d.quantity, s.box_id, b.box_quantity
HAVING gift = 1
