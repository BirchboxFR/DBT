SELECT t.*,
'[Edit](' || t.edit_page_url || ')' AS Edit,
'[View](' || t.view_page_url || ')' AS View,
CASE WHEN t.purchase_currency = 'EUR' THEN t.purchase_price
     WHEN t.purchase_currency = 'GBP' THEN t.purchase_price / 0.864
     WHEN t.purchase_currency IN ('USD','DOL')  THEN t.purchase_price / 1.21
     ELSE 0
END AS euro_purchase_price
FROM
(
    SELECT
    p.dw_country_code,
    p.id AS product_id,
    p.post_id AS product_post_id,
    p.inventory_item_id,
    p.brand_id,
    p.sku,
    p.EAN,
    pnn.brand_full_name,
        case when pnn.product_nice_name is null or pnn.product_nice_name='' then p.name else pnn.product_nice_name end  AS product_nice_name,
    p.price as sale_price,
    p.sales_count,
    p.stock,
    p.stock_physique,
    p.stock_scamp,
    p.parent_post_id,
    p.tampon,
    p.product_codification_id,
    pc.category_lvl_1 AS pc_cat1,
    pc.category_lvl_2 AS pc_cat2,
    pc.category_lvl_1 AS codification,
    CONCAT(pc.category_lvl_1, '/', pc.category_lvl_2) AS full_codification,
    apc.product_categories AS planning_category,
    dpc.category_lvl_1,
    dpc.category_lvl_2,
    dpc.category_lvl_3,
    apc.category_lvl AS planning_category_level,
    apc.parent_lvl1 AS planning_category_lvl_1,
    apc.parent_lvl2 AS planning_category_lvl_2,
    p.visible,
    pp.post_date AS created_at,
    p.url AS view_page_url,
    CASE WHEN p.post_id > 0 THEN CONCAT('https://back.blissim.', LOWER(p.dw_country_code), '/wp-admin/post.php?action=edit&post=', p.post_id) END AS edit_page_url,
    CASE WHEN p.brand_id > 0 THEN CONCAT('https://back.blissim.', LOWER(p.dw_country_code), '/wp-admin/post.php?action=edit&post=', p.brand_id) END AS edit_brand_url,
    COALESCE(ii.discounted_purchase_price, ii.purchase_price, kc.total_cost) AS purchase_price,
    CASE WHEN ii.purchase_currency = '' THEN 'EUR' ELSE ii.purchase_currency END AS purchase_currency,
    p.product_categories_lvl1 AS categories_level_1,
    p.product_categories_lvl2 AS categories_level_2,
    p.product_categories_lvl3 AS categories_level_3,
    p.grade,
    CASE WHEN b.attr_point_rouge IS NULL THEN 0 ELSE b.attr_point_rouge END AS point_rouge,
    REPLACE(p.thumb_url, '40x60', '360x540') AS thumb_url,
    COALESCE(ii.company_id, 1) AS company_id,
    COALESCE(cp.name, 'Blissim') AS company,
    p.attr_brand_code,
    CASE WHEN p.weight > 0 THEN p.weight
         WHEN (p.weight IS NULL OR p.weight = 0) AND p.product_codification_id = 0 THEN 170 -- avg weight of shop products
         WHEN (p.weight IS NULL OR p.weight = 0) AND p.product_codification_id IN (2, 8) THEN 950 -- avg weight of LTE, Splendist Products
         WHEN (p.weight IS NULL OR p.weight = 0) AND p.product_codification_id = 13 THEN 2900 -- avg weight of LTE, Splendist Products
         WHEN (p.weight IS NULL OR p.weight = 0) AND p.product_codification_id = 40 THEN 150 -- loyalty without weight
         WHEN p.weight IS NULL OR p.weight = 0 THEN 200 -- default weight
         ELSE p.weight
    END AS weight,
    tp.taux as vat_rate,
    p.price*(100-tp.taux)/100 AS ht_sale_price
    FROM {{ ref('products') }} p
    LEFT JOIN {{ ref('kit_costs') }} kc ON p.inventory_item_id = kc.inventory_item_id AND p.dw_country_code = kc.country_code
    LEFT JOIN {{ ref('brands') }} b ON p.brand_id = b.post_id AND p.dw_country_code = b.dw_country_code
    LEFT JOIN {{ ref('posts') }} pp ON pp.id = p.post_id AND pp.dw_country_code = p.dw_country_code
    LEFT JOIN {{ ref('nice_names') }} pnn ON pnn.product_id = p.id AND pnn.dw_country_code = p.dw_country_code
    LEFT JOIN {{ ref('brands') }} b_group ON b_group.post_id = b.attr_group_post_id AND b_group.dw_country_code = b.dw_country_code
    LEFT JOIN {{ ref('product_codification') }} pc ON pc.id = p.product_codification_id AND pc.dw_country_code = p.dw_country_code
    LEFT JOIN {{ ref('algolia_product_categories') }} apc ON apc.term_id = p.attr_planning_category AND apc.dw_country_code = p.dw_country_code
    LEFT JOIN {{ ref('categories') }} dpc ON dpc.term_id = p.attr_planning_category AND dpc.dw_country_code = p.dw_country_code
    LEFT JOIN {{ ref('inventory_items') }} ii ON ii.id = p.inventory_item_id AND ii.dw_country_code = p.dw_country_code
    LEFT JOIN {{ ref('company') }} cp ON cp.id = ii.company_id AND cp.dw_country_code = ii.dw_country_code
    LEFT JOIN {{ ref('tva_product') }} tp ON tp.dw_country_code = p.dw_country_code AND tp.country_code = p.dw_country_code AND tp.category = CASE WHEN p.attr_tva_type IN ('alimentaire', 'hygienique') THEN p.attr_tva_type ELSE 'normal' END 
) t
GROUP BY t.dw_country_code, t.product_id, t.product_post_id, t.inventory_item_id, t.brand_id, t.sku, t.ean, t.brand_full_name, t.product_nice_name,
     t.sales_count, t.stock, t.stock_physique, t.tampon, t.product_codification_id, t.codification, t.full_codification, t.planning_category, t.planning_category_level,
     t.planning_category_lvl_1, t.planning_category_lvl_2, t.visible, t.created_at, t.view_page_url, t.edit_page_url, t.edit_brand_url, t.purchase_currency, 
     t.purchase_price, t.categories_level_1, t.categories_level_2, t.categories_level_3, t.grade, edit, view, euro_purchase_price, point_rouge,
     t.category_lvl_1, t.category_lvl_2, t.category_lvl_3, t.thumb_url, t.company_id, t.company, pc_cat1, pc_cat2, t.attr_brand_code, t.stock_scamp, t.weight,sale_price, vat_rate,ht_sale_price,parent_post_id