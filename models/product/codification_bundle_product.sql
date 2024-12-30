WITH bundle_with_gift_table AS (
  SELECT pbc.dw_country_code,
         pbc.bundle_product_id,
         MAX(p_component.product_codification_id = 34) AS bundle_with_gift
  FROM inter.products_bundle_component pbc
  INNER JOIN inter.products p_component ON pbc.component_product_id = p_component.id AND pbc.dw_country_code = p_component.dw_country_code
  INNER JOIN inter.products p_bundle ON pbc.bundle_product_id = p_bundle.id AND pbc.dw_country_code = p_bundle.dw_country_code AND p_bundle.attr_is_bundle = 1
  GROUP BY pbc.dw_country_code,
           pbc.bundle_product_id
  HAVING bundle_with_gift
)
SELECT t.*, pc.category_lvl_1 AS product_codification
FROM (
  SELECT DISTINCT pbc.dw_country_code,
         pbc.bundle_product_id,
         pbc.component_product_id,
         CASE WHEN bwgt.bundle_product_id IS NULL
              THEN p_bundle.product_codification_id
              ELSE p_component.product_codification_id
         END AS product_codification_id
  FROM inter.products_bundle_component pbc
  INNER JOIN inter.products p_component ON pbc.component_product_id = p_component.id AND pbc.dw_country_code = p_component.dw_country_code
  INNER JOIN inter.products p_bundle ON pbc.bundle_product_id = p_bundle.id AND pbc.dw_country_code = p_bundle.dw_country_code AND p_bundle.attr_is_bundle = 1
  LEFT JOIN bundle_with_gift_table bwgt ON pbc.dw_country_code = bwgt.dw_country_code AND pbc.bundle_product_id = bwgt.bundle_product_id
) t
INNER JOIN inter.product_codification pc ON t.dw_country_code = pc.dw_country_code AND t.product_codification_id = pc.id
