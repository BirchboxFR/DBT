SELECT k.dw_country_code, o.user_id, s.box_id, k.product_id as sample_id
FROM inter.kit_links k
JOIN inter.products pbox ON k.kit_id = pbox.ID AND pbox.dw_country_code = k.dw_country_code
JOIN inter.products prod ON prod.id = k.product_id AND prod.dw_country_code = k.dw_country_code
JOIN inter.order_detail_sub s ON s.box_id = pbox.box_id AND s.coffret_id = pbox.coffret_id AND s.dw_country_code = pbox.dw_country_code
JOIN inter.order_details d ON d.id = s.order_detail_id AND d.dw_country_code = s.dw_country_code
JOIN inter.orders o ON o.id = d.order_id AND o.dw_country_code = d.dw_country_code
GROUP BY k.dw_country_code, o.user_id, s.box_id, k.product_id