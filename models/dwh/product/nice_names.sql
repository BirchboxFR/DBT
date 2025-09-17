SELECT p.dw_country_code,
       p.id AS product_id,
       g.post_title AS brand_group_name,
       IF(b.visible = 0, CONCAT(b.name, ' (disabled)'), b.name) AS brand_name,
       CONCAT(IF(g.post_title IS NULL, '', CONCAT(g.post_title, ' - ')), IF((b.visible = 0), CONCAT(b.name, ' (disabled)'), b.name)) AS brand_full_name,
       REGEXP_REPLACE(CASE
                           WHEN p_product.post_title IS NULL THEN p.name
                           WHEN p.is_parent  AND (COALESCE(p.nb_children, 0) = 0) THEN p_product.post_title
                           WHEN p.is_parent  AND (p.nb_children > 0) THEN CONCAT(p_product_parent.post_title, ' [Parent]')
                           WHEN p.post_id <> p.parent_post_id THEN CONCAT(p_product_parent.post_title, ' | ', p_product.post_title)
                           ELSE p_product.post_title
                      END, r'\&.*;', '') AS product_nice_name
FROM {{ ref('products') }} p
LEFT JOIN {{ ref('posts') }} p_product ON p_product.ID = p.post_id AND p.dw_country_code = p_product.dw_country_code
LEFT JOIN {{ ref('posts') }} p_product_parent ON p_product_parent.ID = p.parent_post_id AND p_product_parent.dw_country_code = p.dw_country_code
LEFT JOIN {{ ref('brands') }} b ON p.brand_id = b.post_id AND p.dw_country_code = b.dw_country_code
LEFT JOIN {{ ref('posts') }} g ON g.ID = b.attr_group_post_id AND g.dw_country_code = b.dw_country_code
