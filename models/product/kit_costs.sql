WITH details_product AS (
  SELECT ak.country_code,
         ii.id AS inventory_item_id,kit_id,
         MAX(COALESCE(DATE(po.post_date), DATE(p1.created_at))) AS ii_date,
         MAX(p1.special_type) AS special_type,
         COUNTIF(pc.category_lvl_2 = 'Product') AS nb_products,
         SUM(IF(pc.category_lvl_2 = 'Product', COALESCE(p2.attr_discounted_purchase_price, p2.attr_purchase_price), 0)) AS coop,
         COUNTIF(pc.category_lvl_2 = 'Product' AND COALESCE(p2.attr_discounted_purchase_price, p2.attr_purchase_price) IS NULL) AS nb_missing_product_price,
         COUNTIF(pc.category_lvl_2 = 'Pack') AS nb_packs,
         SUM(IF(pc.category_lvl_2 = 'Pack', COALESCE(p2.attr_discounted_purchase_price, p2.attr_purchase_price), 0)) AS pack_cost,
         COUNTIF(pc.category_lvl_2 = 'Pack' AND COALESCE(p2.attr_discounted_purchase_price, p2.attr_purchase_price) IS NULL) AS nb_missing_pack_price,
         COUNTIF(pc.category_lvl_2 = 'Print') AS nb_prints,
         SUM(IF(pc.category_lvl_2 = 'Print', COALESCE(p2.attr_discounted_purchase_price, p2.attr_purchase_price), 0)) AS print_cost,
         COUNTIF(pc.category_lvl_2 = 'Print' AND COALESCE(p2.attr_discounted_purchase_price, p2.attr_purchase_price) IS NULL) AS nb_missing_print_price,
         COUNTIF(pc.category_lvl_2 = 'Consumable item') AS nb_consumables,
         SUM(IF(pc.category_lvl_2 = 'Consumable item', COALESCE(p2.attr_discounted_purchase_price, p2.attr_purchase_price), 0)) AS consumable_cost,
         COUNTIF(pc.category_lvl_2 = 'Consumable item' AND COALESCE(p2.attr_discounted_purchase_price, p2.attr_purchase_price) IS NULL) AS nb_missing_consumable_item_price,
         MAX(ak.kit_type) AS kit_type
  FROM product.all_kits ak
  INNER JOIN inter.products p1 ON ak.country_code = p1.dw_country_code AND ak.kit_id = p1.id
  LEFT JOIN inter.posts po ON p1.dw_country_code = po.dw_country_code AND p1.post_id = po.id
  INNER JOIN inter.products p2 ON ak.country_code = p2.dw_country_code AND ak.product_id = p2.id
  INNER JOIN inter.product_codification pc ON p2.dw_country_code = pc.dw_country_code AND p2.product_codification_id = pc.id
  INNER JOIN inter.inventory_items ii ON p1.dw_country_code = ii.dw_country_code AND p1.inventory_item_id = ii.id
  GROUP BY all
),
wout_total_costs AS (
  SELECT dp.country_code,kit_id,
         dp.inventory_item_id,
         ROUND(SUM(CASE WHEN dlc.name = 'assembly basis lte' AND dp.special_type = 'LTE' THEN price ELSE 0 END
         + CASE WHEN dlc.name = 'assembly basis' THEN price ELSE 0 END
         + CASE WHEN dlc.name = 'assembly product supp' THEN GREATEST(nb_products - 5, 0) * price ELSE 0 END
         + CASE WHEN dlc.name = 'assembly pack supp' THEN GREATEST(nb_packs - 2, 0) * price ELSE 0 END
         + CASE WHEN dlc.name = 'assembly print supp' THEN GREATEST(nb_prints - 1, 0) * price ELSE 0 END), 3) AS assembly_cost,
         MAX(ROUND(coop, 3)) AS coop,
         MAX(ROUND(pack_cost, 3)) AS pack_cost,
         MAX(ROUND(print_cost, 3)) AS print_cost,
         MAX(ROUND(consumable_cost, 3)) AS consumable_cost,
         MAX(nb_missing_product_price) AS nb_missing_product_price,
         MAX(nb_missing_pack_price) AS nb_missing_pack_price,
         MAX(nb_missing_print_price) AS nb_missing_print_price,
         MAX(nb_missing_consumable_item_price) AS nb_missing_consumable_item_price,
         MAX(kit_type) AS kit_type,
         MAX(nb_products) AS nb_products,
         MAX(nb_packs) AS nb_packs
  FROM details_product dp
  INNER JOIN ops.logistics_costs dlc ON dp.ii_date >= DATE(dlc.date_start) AND (dp.ii_date <= DATE(dlc.date_end) OR date_end IS NULL)
  WHERE dlc.name IN ('assembly basis',
                     'assembly basis lte',
                     'assembly product supp',
                     'assembly pack supp',
                     'assembly print supp')
  GROUP BY ALL
),
default_costs AS (
  SELECT 5 AS nb_products,
         2 AS nb_packs,
         2.1 AS default_coop,
         0.65 AS default_pack_cost
  UNION ALL
  SELECT 10 AS nb_products,
         2 AS nb_packs,
         3.41 AS default_coop,
         1.63 AS default_pack_cost
)
SELECT country_code,
       inventory_item_id,kit_id,
       ROUND(assembly_cost + coop + pack_cost + print_cost + consumable_cost, 3) AS total_cost,
       assembly_cost,
       CASE WHEN wtc.nb_missing_product_price = 0 THEN wtc.coop
            ELSE COALESCE(dc.default_coop, wtc.coop)
       END AS coop,
       CASE WHEN wtc.nb_missing_pack_price = 0 THEN wtc.pack_cost
            ELSE COALESCE(dc.default_pack_cost, wtc.pack_cost)
       END AS pack_cost,
       print_cost,
       consumable_cost,
       nb_missing_product_price,
       nb_missing_pack_price,
       nb_missing_print_price,
       nb_missing_consumable_item_price,
       wtc.nb_missing_product_price > 0 AND dc.default_coop IS NOT NULL AS is_default_coop,
       wtc.nb_missing_pack_price > 0 AND dc.default_pack_cost IS NOT NULL AS is_default_pack_cost
FROM wout_total_costs wtc
LEFT JOIN default_costs dc ON wtc.nb_products = dc.nb_products AND wtc.nb_packs = dc.nb_packs AND wtc.kit_type = 'BOX'
