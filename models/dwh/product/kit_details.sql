SELECT kl.dw_country_code, kit.product_id AS kit_product_id, kit.sku AS kit_sku, p.box_id, p.coffret_id, kit.brand_full_name AS kit_brand_name, kit.brand_id AS kit_brand_id, kit.product_codification_id AS kit_codification_id, pc_kit.category_lvl_1 AS kit_codification_lvl1, pc_kit.category_lvl_2 AS kit_codification_lvl2,
b.date as box_date, extract(year from b.date) AS box_year, extract(month from b.date) as box_month,
component.product_id AS component_product_id, component.sku as component_sku, component.brand_id as component_brand_id, component.brand_full_name as component_brand_name, component.product_nice_name AS component_name, component.product_codification_id AS component_codification_id, pc_component.category_lvl_1 AS component_codification_lvl1,pc_component.category_lvl_2 AS component_codification_lvl2, component.euro_purchase_price as component_euro_purchase_price
FROM {{ ref('kit_links') }}  kl
JOIN {{ ref('catalog') }} as kit ON kit.product_id = kl.kit_id AND kit.dw_country_code = kl.dw_country_code
JOIN {{ ref('products') }} p ON p.id = kit.product_id AND kit.dw_country_code = p.dw_country_code
JOIN {{ ref('catalog') }} as component ON component.product_id = kl.product_id AND component.dw_country_code = kl.dw_country_code
LEFT JOIN {{ ref('boxes') }} b ON b.id = p.box_id AND b.dw_country_code = p.dw_country_code
LEFT JOIN {{ ref('product_codification') }}  pc_kit ON pc_kit.id = kit.product_codification_id AND pc_kit.dw_country_code = kit.dw_country_code
LEFT JOIN {{ ref('product_codification') }} pc_component ON pc_component.id = component.product_codification_id AND pc_component.dw_country_code = component.dw_country_code

UNION ALL

SELECT kl.dw_country_code, kit.product_id AS kit_product_id, kit.sku AS kit_sku, p.box_id, p.coffret_id, kit.brand_full_name AS kit_brand_name, kit.brand_id AS kit_brand_id, kit.product_codification_id AS kit_codification_id, pc_kit.category_lvl_1 AS kit_codification_lvl1, pc_kit.category_lvl_2 AS kit_codification_lvl2,
NULL as box_date, NULL AS box_year, NULL as box_month,
component.product_id AS component_product_id, component.sku as component_sku, component.brand_id as component_brand_id, component.brand_full_name as component_brand_name, component.product_nice_name AS component_name, component.product_codification_id AS component_codification_id, pc_component.category_lvl_1 AS component_codification_lvl1,pc_component.category_lvl_2 AS component_codification_lvl2, component.euro_purchase_price as component_euro_purchase_price
FROM {{ ref('lte_kits') }} kl
JOIN {{ ref('catalog') }} as kit ON kit.product_id = kl.lte_product_id AND kit.dw_country_code = kl.dw_country_code
JOIN {{ ref('products') }} p ON p.id = kit.product_id AND kit.dw_country_code = p.dw_country_code
JOIN {{ ref('catalog') }} as component ON component.product_id = kl.product_id AND component.dw_country_code = kl.dw_country_code
LEFT JOIN {{ ref('product_codification') }} pc_kit ON pc_kit.id = kit.product_codification_id AND pc_kit.dw_country_code = kit.dw_country_code
LEFT JOIN {{ ref('product_codification') }} pc_component ON pc_component.id = component.product_codification_id AND pc_component.dw_country_code = component.dw_country_code
WHERE p.box_id IS NULL
