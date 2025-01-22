
SELECT 'FR' AS dw_country_code, p.*except(attr_special_price_start,attr_special_price_end,attr_free_shipping_start,attr_free_shipping_end, attr_shipping_delayed_to),
safe_cast(attr_special_price_start as date) as attr_special_price_start,
safe_cast(attr_special_price_end as date) as attr_special_price_end,
safe_cast(attr_free_shipping_start as date) as attr_free_shipping_start,
safe_cast(attr_free_shipping_end as date) as attr_free_shipping_end,
safe_cast(attr_shipping_delayed_to as date) as attr_shipping_delayed_to
 FROM `bdd_prod_fr.wp_jb_products` p
UNION ALL 
SELECT 'DE' AS dw_country_code,  p.*except(attr_special_price_start,attr_special_price_end,attr_free_shipping_start,attr_free_shipping_end, attr_shipping_delayed_to),
safe_cast(attr_special_price_start as date) as attr_special_price_start,
safe_cast(attr_special_price_end as date) as attr_special_price_end,
safe_cast(attr_free_shipping_start as date) as attr_free_shipping_start,
safe_cast(attr_free_shipping_end as date) as attr_free_shipping_end,
safe_cast(attr_shipping_delayed_to as date) as attr_shipping_delayed_to FROM `bdd_prod_de.wp_jb_products` p
UNION ALL 
SELECT 'ES' AS dw_country_code,  p.*except(attr_special_price_start,attr_special_price_end,attr_free_shipping_start,attr_free_shipping_end, attr_shipping_delayed_to),
safe_cast(attr_special_price_start as date) as attr_special_price_start,
safe_cast(attr_special_price_end as date) as attr_special_price_end,
safe_cast(attr_free_shipping_start as date) as attr_free_shipping_start,
safe_cast(attr_free_shipping_end as date) as attr_free_shipping_end,
safe_cast(attr_shipping_delayed_to as date) as attr_shipping_delayed_to FROM `bdd_prod_es.wp_jb_products` p
UNION ALL 
SELECT 'IT' AS dw_country_code,  p.*except(attr_special_price_start,attr_special_price_end,attr_free_shipping_start,attr_free_shipping_end, attr_shipping_delayed_to),
safe_cast(attr_special_price_start as date) as attr_special_price_start,
safe_cast(attr_special_price_end as date) as attr_special_price_end,
safe_cast(attr_free_shipping_start as date) as attr_free_shipping_start,
safe_cast(attr_free_shipping_end as date) as attr_free_shipping_end,
safe_cast(attr_shipping_delayed_to as date) as attr_shipping_delayed_to FROM `bdd_prod_it.wp_jb_products` p