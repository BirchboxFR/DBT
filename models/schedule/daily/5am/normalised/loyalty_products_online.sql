
  SELECT c.dw_country_code,
  safe_cast(points_category.meta_value  as int64)meta_value,
  c.stock,
  loyalty_type.meta_value AS loyalty_type,
  safe_cast(value.meta_value as int64)AS value,
  c.thumb_url AS image,
  c.product_id,
  c.sku, 
  c.brand_full_name,
  c.product_nice_name,
  c.planning_category,
  c.euro_purchase_price
  FROM product.catalog c
  LEFT JOIN inter.postmeta points_category ON points_category.post_id = c.product_post_id AND points_category.meta_key = 'attr_product_loyalty_points' and points_category.dw_country_code=c.dw_country_code
  LEFT JOIN inter.postmeta loyalty_type ON loyalty_type.post_id = c.product_post_id AND loyalty_type.meta_key = 'loyalty_product_type ' and loyalty_type.dw_country_code=c.dw_country_code
  LEFT JOIN inter.postmeta display ON display.post_id = c.product_post_id AND display.meta_key = 'attr_product_display_in_reward' and display.dw_country_code=c.dw_country_code
  LEFT JOIN inter.postmeta value ON value.post_id = c.product_post_id AND value.meta_key = 'value_loyalty' and value.dw_country_code=c.dw_country_code

  WHERE 1=1
  AND c.product_codification_id = 40
  and c.dw_country_code='FR'
  AND c.visible = 'catalog_and_search'
  AND c.stock > 0
  AND display.meta_value = '1'
  GROUP BY all
  having meta_value >70
order by meta_value desc