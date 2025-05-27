/*

SELECT o.dw_country_code, o.user_id, s.box_id, b.date
FROM {{ ref('orders') }} o
INNER JOIN {{ ref('order_details') }} d ON o.id = d.order_id AND o.dw_country_code = d.dw_country_code
INNER JOIN {{ ref('order_detail_sub') }} s ON s.order_detail_id = d.id AND s.dw_country_code = d.dw_country_code
INNER JOIN {{ ref('boxes') }} b ON b.id = s.box_id AND b.dw_country_code = s.dw_country_code
LEFT JOIN sales.box_sales bs ON bs.dw_country_code = o.dw_country_code AND bs.user_id = o.user_id AND bs.box_id = s.box_id
LEFT JOIN {{ ref('box_paused') }} bp ON bp.dw_country_code = o.dw_country_code AND bp.user_id = o.user_id AND bp.box_id = s.box_id
WHERE o.status_id IN (1,3) 
AND s.cannot_suspend = 1
AND bs.user_id IS NULL
AND bp.user_id IS NULL
GROUP BY ALL

*/
select 1