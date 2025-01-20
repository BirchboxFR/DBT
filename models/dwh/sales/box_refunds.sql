SELECT t.*,
- t.total_product / (1+vat_rate/100) AS gross_revenue,
- (t.total_product - t.total_product/(1+vat_rate/100)) AS vat_on_gross_revenue,
- t.total_discount / (1+vat_rate/100) AS discount,
- (t.total_discount - t.total_discount / (1+vat_rate/100)) AS vat_on_discount,
- t.total_shipping/(1+vat_rate/100) AS shipping,
- (t.total_shipping - t.total_shipping/(1+vat_rate/100)) AS vat_on_shipping
FROM
(
--   ------------------ BOX TOTAL Refunds --------------------------
SELECT an.dw_country_code,
s.id AS sub_id,
d.id AS order_detail_id,
o.id AS order_id,
o.user_id,
s.box_id,
b.date,
an.eventdate AS payment_date,
EXTRACT(month FROM b.date) AS month,
EXTRACT(year FROM b.date) AS year,
s.shipping_mode,
CASE WHEN d.gift_card_id = 0 OR (d.gift_card_id > 0 AND (s.box_id - d.sub_start_box >= d.quantity)) THEN 1 ELSE 0 END AS self,
CASE WHEN d.gift_card_id > 0 AND (s.box_id - d.sub_start_box < d.quantity) THEN 1 ELSE 0 END AS gift,
CASE WHEN yc.yearly_coupon_id IS NOT NULL THEN 1 ELSE 0 END AS yearly,
CASE WHEN d.quantity = -12 THEN 1 ELSE 0 END AS old_yearly,
d.quantity AS dquantity,
s.cannot_suspend AS cannot_suspend,
CASE WHEN s.total_product = 0 AND gc.id IS NULL THEN 0
     WHEN s.total_product = 0 AND gc.id IS NOT NULL THEN gc.amount/gc.duration
     ELSE s.total_product 
END AS total_product,
an.dw_country_code AS store_code,
COALESCE(tva.taux, 0) AS vat_rate,
CASE WHEN c.dw_country_code = 'FR' AND c.parent_id = 15237671 AND s.box_id = d.sub_start_box THEN 0.0 -- Veepee offer - May 2021
     WHEN so.dw_country_code = 'FR' AND so.parent_offer_id = 53382 THEN 0.0 -- Veepee offer - May 2021
     ELSE s.total_discount END AS total_discount,
s.shipping_country AS shipping_country,
s.total_shipping AS total_shipping,
CASE WHEN s.sub_payment_status_id = 3 THEN 'forthcoming' ELSE 'paid' END AS payment_status,
sps.name AS sub_payment_status,
d.sub_start_box
FROM {{ ref('adyen_notifications') }} an
INNER JOIN {{ ref('orders') }} o ON o.id = an.order_id AND o.dw_country_code = an.dw_country_code
INNER JOIN {{ ref('order_detail_sub') }} s ON s.id = an.sub_id AND s.dw_country_code = an.dw_country_code
INNER JOIN {{ ref('order_details') }} d ON d.id = s.order_detail_id AND d.dw_country_code = s.dw_country_code
INNER JOIN {{ ref('boxes') }} b ON b.id = s.box_id AND b.dw_country_code = s.dw_country_code
LEFT JOIN {{ ref('gift_cards') }} gc ON gc.ID = d.gift_card_id AND gc.dw_country_code = d.dw_country_code
LEFT JOIN {{ ref('coupons') }} c ON c.id = o.coupon_code_id AND c.dw_country_code = o.dw_country_code
LEFT JOIN {{ ref('sub_offers') }} so ON so.id = s.sub_offer_id AND so.dw_country_code = s.dw_country_code
INNER JOIN bdd_prod_fr.wp_jb_sub_payments_status sps ON sps.id = s.sub_payment_status_id
LEFT JOIN inter.tva_product tva ON tva.country_code = s.shipping_country AND tva.category = 'normal' AND tva.dw_country_code = s.dw_country_code
LEFT JOIN snippets.yearly_coupons yc ON an.dw_country_code = yc.country_code AND o.coupon_code_id = yc.yearly_coupon_id
WHERE an.eventdate >= '2018-01-01'
AND an.eventCode IN ('REFUND', 'CANCEL_OR_REFUND')
AND an.success = 1
AND an.sub_id > 0
AND ABS(1.0*an.value/100 - s.total) < 0.03

UNION ALL

-- ---------------- shipping refunds  ------------------------------
SELECT an.dw_country_code,
s.id AS sub_id,
d.id AS order_detail_id,
o.id AS order_id,
o.user_id, 
s.box_id,
b.date,
an.eventdate AS payment_date,
EXTRACT(month FROM b.date) AS month,
EXTRACT(year FROM b.date) AS year,
s.shipping_mode,
CASE WHEN d.gift_card_id = 0 OR (d.gift_card_id > 0 AND (s.box_id - d.sub_start_box >= d.quantity)) THEN 1 ELSE 0 END AS self,
CASE WHEN d.gift_card_id > 0 AND (s.box_id - d.sub_start_box < d.quantity) THEN 1 ELSE 0 END AS gift,
CASE WHEN yc.yearly_coupon_id IS NOT NULL THEN 1 ELSE 0 END AS yearly,
CASE WHEN d.quantity = -12 THEN 1 ELSE 0 END AS old_yearly,
d.quantity AS dquantity,
s.cannot_suspend AS cannot_suspend,
0 AS total_product,
an.dw_country_code AS store_code,
COALESCE(tva.taux, 0) AS vat_rate,
0 AS total_discount,
s.shipping_country AS shipping_country,
1.0*an.value/100 AS total_shipping,
CASE WHEN s.sub_payment_status_id = 3 THEN 'forthcoming' ELSE 'paid' END AS payment_status,
sps.name AS sub_payment_status,
d.sub_start_box
FROM {{ ref('adyen_notifications') }} an
INNER JOIN {{ ref('orders') }} o ON o.id = an.order_id AND o.dw_country_code = an.dw_country_code
INNER JOIN {{ ref('order_detail_sub') }} s ON s.id = an.sub_id AND s.dw_country_code = an.dw_country_code
INNER JOIN {{ ref('order_details') }} d ON d.id = s.order_detail_id AND d.dw_country_code = s.dw_country_code
INNER JOIN {{ ref('boxes') }} b ON b.id = s.box_id AND b.dw_country_code = s.dw_country_code
LEFT JOIN {{ ref('gift_cards') }} gc ON gc.ID = d.gift_card_id AND gc.dw_country_code = d.dw_country_code
LEFT JOIN {{ ref('coupons') }} c ON c.id = o.coupon_code_id AND c.dw_country_code = o.dw_country_code
LEFT JOIN {{ ref('sub_offers') }} so ON so.id = s.sub_offer_id AND so.dw_country_code = s.dw_country_code
INNER JOIN bdd_prod_fr.wp_jb_sub_payments_status sps ON sps.id = s.sub_payment_status_id
LEFT JOIN inter.tva_product tva ON tva.country_code = s.shipping_country AND tva.category = 'normal' AND tva.dw_country_code = s.dw_country_code
LEFT JOIN snippets.yearly_coupons yc ON an.dw_country_code = yc.country_code AND o.coupon_code_id = yc.yearly_coupon_id
WHERE an.eventdate >= '2018-01-01'
AND an.eventCode IN ('REFUND', 'CANCEL_OR_REFUND')
AND an.success = 1
AND an.sub_id > 0
AND abs(1.0*an.value/100 - s.total) >= 0.03
AND 1.0*an.value/100 <= s.total_shipping
  
UNION ALL
  
-- ---------------- partial box refunds  ------------------------------
  
SELECT an.dw_country_code,
s.id AS sub_id,
d.id AS order_detail_id,
o.id AS order_id,
o.user_id, 
s.box_id,
b.date,
an.eventdate AS payment_date,
EXTRACT(month FROM b.date) AS month,
EXTRACT(year FROM b.date) AS year,
s.shipping_mode,
CASE WHEN d.gift_card_id = 0 OR (d.gift_card_id > 0 AND (s.box_id - d.sub_start_box >= d.quantity)) THEN 1 ELSE 0 END AS self,
CASE WHEN d.gift_card_id > 0 AND (s.box_id - d.sub_start_box < d.quantity) THEN 1 ELSE 0 END AS gift,
CASE WHEN yc.yearly_coupon_id IS NOT NULL THEN 1 ELSE 0 END AS yearly,
CASE WHEN d.quantity = -12 THEN 1 ELSE 0 END AS old_yearly,
d.quantity AS dquantity,
s.cannot_suspend AS cannot_suspend,
1.0*an.value/100 AS total_product,
an.dw_country_code AS store_code,
COALESCE(tva.taux, 0) AS vat_rate,
0 AS total_discount,
s.shipping_country AS shipping_country,
0 AS total_shipping,
CASE WHEN s.sub_payment_status_id = 3 THEN 'forthcoming' ELSE 'paid' END AS payment_status,
sps.name AS sub_payment_status,
d.sub_start_box
FROM {{ ref('adyen_notifications') }} an
INNER JOIN {{ ref('orders') }} o ON o.id = an.order_id AND o.dw_country_code = an.dw_country_code
INNER JOIN {{ ref('order_detail_sub') }} s ON s.id = an.sub_id AND s.dw_country_code = an.dw_country_code
INNER JOIN {{ ref('order_details') }} d ON d.id = s.order_detail_id AND d.dw_country_code = s.dw_country_code
INNER JOIN {{ ref('boxes') }} b ON b.id = s.box_id AND b.dw_country_code = s.dw_country_code
LEFT JOIN {{ ref('gift_cards') }} gc ON gc.ID = d.gift_card_id AND gc.dw_country_code = d.dw_country_code
LEFT JOIN {{ ref('coupons') }} c ON c.id = o.coupon_code_id AND c.dw_country_code = o.dw_country_code
LEFT JOIN {{ ref('sub_offers') }} so ON so.id = s.sub_offer_id AND so.dw_country_code = s.dw_country_code
INNER JOIN bdd_prod_fr.wp_jb_sub_payments_status sps ON sps.id = s.sub_payment_status_id
LEFT JOIN inter.tva_product tva ON tva.country_code = s.shipping_country AND tva.category = 'normal' AND tva.dw_country_code = s.dw_country_code
LEFT JOIN snippets.yearly_coupons yc ON an.dw_country_code = yc.country_code AND o.coupon_code_id = yc.yearly_coupon_id
WHERE an.eventdate >= '2018-01-01'
AND an.eventCode IN ('REFUND', 'CANCEL_OR_REFUND')
AND an.success = 1
AND an.sub_id > 0
AND abs(1.0*an.value/100 - s.total) >= 0.03
AND 1.0*an.value/100 > s.total_shipping
  
UNION ALL
  
-- ------------------------ box ORDER refunds--------------------------
SELECT an.dw_country_code,
s.id AS sub_id,
d.id AS order_detail_id,
o.id AS order_id,
o.user_id, 
s.box_id,
b.date,
an.eventdate AS payment_date,
extract(month from b.date) AS month,
extract(year from b.date) AS year,
s.shipping_mode,
CASE WHEN d.gift_card_id = 0 OR (d.gift_card_id > 0 AND (s.box_id - d.sub_start_box >= d.quantity)) THEN 1 ELSE 0 END AS self,
CASE WHEN d.gift_card_id > 0 AND (s.box_id - d.sub_start_box < d.quantity) THEN 1 ELSE 0 END AS gift,
CASE WHEN yc.yearly_coupon_id IS NOT NULL THEN 1 ELSE 0 END AS yearly,
CASE WHEN d.quantity= -12 THEN 1 ELSE 0 END AS old_yearly,
d.quantity AS dquantity,
s.cannot_suspend AS cannot_suspend,
CASE  WHEN s.total_product = 0 AND gc.id IS NULL THEN 0
      WHEN s.total_product = 0 AND gc.id IS NOT NULL THEN gc.amount/gc.duration
      ELSE s.total_product 
END AS total_product,
an.dw_country_code AS store_code,
COALESCE(tva.taux, 0) AS vat_rate,
CASE WHEN c.dw_country_code = 'FR' AND c.parent_id = 15237671 AND s.box_id = d.sub_start_box THEN 0.0 -- Veepee offer - May 2021
     WHEN so.dw_country_code = 'FR' AND so.parent_offer_id = 53382 THEN 0.0 -- Veepee offer - May 2021
     ELSE s.total_discount
END AS total_discount,
s.shipping_country AS shipping_country,
s.total_shipping AS total_shipping,
CASE WHEN s.sub_payment_status_id = 3 THEN 'forthcoming' ELSE 'paid' END AS payment_status,
sps.name AS sub_payment_status,
d.sub_start_box
FROM {{ ref('adyen_notifications') }} an
INNER JOIN {{ ref('orders') }} o ON o.id = an.order_id AND o.dw_country_code = an.dw_country_code
INNER JOIN {{ ref('order_details') }} d ON d.order_id = o.id AND d.product_id = 1 AND d.dw_country_code = o.dw_country_code
INNER JOIN {{ ref('order_detail_sub') }} s ON s.order_detail_id = d.id AND s.box_id = d.sub_start_box AND s.dw_country_code = d.dw_country_code
INNER JOIN {{ ref('boxes') }} b ON b.id = s.box_id AND b.dw_country_code = s.dw_country_code
LEFT JOIN {{ ref('gift_cards') }} gc ON gc.ID = d.gift_card_id AND gc.dw_country_code = d.dw_country_code
LEFT JOIN {{ ref('coupons') }} c ON c.id = o.coupon_code_id AND c.dw_country_code = o.dw_country_code
LEFT JOIN {{ ref('sub_offers') }} so ON so.id = s.sub_offer_id AND so.dw_country_code = s.dw_country_code
INNER JOIN bdd_prod_fr.wp_jb_sub_payments_status sps ON sps.id = s.sub_payment_status_id
LEFT JOIN inter.tva_product tva ON tva.country_code = s.shipping_country AND tva.category = 'normal' AND tva.dw_country_code = s.dw_country_code
LEFT JOIN snippets.yearly_coupons yc ON an.dw_country_code = yc.country_code AND o.coupon_code_id = yc.yearly_coupon_id
WHERE an.eventdate >= '2018-01-01'
AND an.eventCode IN ('REFUND', 'CANCEL_OR_REFUND')
AND an.success = 1
AND (an.sub_id IS NULL OR an.sub_id = 0)
AND ABS(1.0*an.value/100 - s.total) < 0.03) t
