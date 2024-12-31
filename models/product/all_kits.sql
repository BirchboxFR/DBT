WITH lte_kits AS (
  SELECT dw_country_code AS country_code, lte_product_id AS kit_id, product_id, COUNT(*) AS quantity, 'LTE' AS kit_type
  FROM inter.lte_kits
  GROUP BY dw_country_code, lte_product_id, product_id
)
SELECT country_code, kit_id, product_id, max(quantity) AS quantity,'BOX' as kit_type
FROM
(
SELECT dw_country_code AS country_code, kit_id, product_id, quantity, 'BOX' AS kit_type
FROM {{ ref('kit_links') }}
UNION ALL
SELECT country_code, kit_id, product_id, quantity, kit_type
FROM lte_kits
) t
GROUP BY country_code, kit_id, product_id
