{{ config(
    materialized='view',
    on_schema_change='ignore'
) }}


WITH sales AS (
  SELECT 'SALES' AS type, product_codification, store_code, shipping_country, year, month, vat_rate, SUM(gross_revenue) AS value, MAX(LAST_DAY(order_date)) AS last_day
  FROM {{ ref('shop_sales') }}
  WHERE product_codification NOT IN ('LOYALTY', 'LOYALTY COUPON', 'GIFT')
  GROUP BY type, product_codification, store_code, shipping_country, year, month, vat_rate
),
discount_wout_loyalty AS (
  SELECT 'DISCOUNT_WOUT_PTS' AS type, product_codification, store_code, shipping_country, year, month, vat_rate, SUM(total_discount - points_discount) AS total_discount, MAX(LAST_DAY(order_date)) AS last_day
  FROM {{ ref('shop_sales') }}
  WHERE product_codification NOT IN ('LOYALTY', 'LOYALTY COUPON', 'GIFT')
  GROUP BY type, product_codification, store_code, shipping_country, year, month, vat_rate
),
discount_loyalty AS (
  SELECT 'POINTS_DISCOUNT' AS type, product_codification, store_code, shipping_country, year, month, vat_rate, SUM(points_discount) AS total_discount, MAX(LAST_DAY(order_date)) AS last_day
  FROM {{ ref('shop_sales') }}
  WHERE product_codification NOT IN ('LOYALTY', 'LOYALTY COUPON', 'GIFT')
  GROUP BY type, product_codification, store_code, shipping_country, year, month, vat_rate
),
shipping_shop AS (
  SELECT 'SHIPPING' as type, 'ESHOP' AS product_codification, store_code, shipping_country, year, month, vat_rate, SUM(order_total_shipping) AS total_shipping, last_day
  FROM (
    SELECT store_code, order_id, ss.shipping_country, ss.year, ss.month, MAX(order_total_shipping) AS order_total_shipping, COALESCE(tva.taux, 0) AS vat_rate, MAX(last_day(order_date)) AS last_day
    FROM {{ ref('shop_sales') }} ss
    LEFT JOIN {{ ref('tva_product') }} tva ON ss.dw_country_code = tva.dw_country_code AND tva.country_code = ss.shipping_country AND tva.category = 'normal'
    GROUP BY store_code, order_id, shipping_country, year, month, tva.taux
  ) t
  GROUP BY store_code, vat_rate, last_day, shipping_country, year, month
),
total_vat AS (
  SELECT t.type, product_cod AS product_codification, t.store_code, t.shipping_country, year, month, t.vat_rate_, SUM(t.vat) AS value, last_day
  FROM
  (
      SELECT ss.order_id, 'VAT' AS type, 'ALL' AS product_cod, store_code, year, month,
      ss.shipping_country,
      vat_rate AS vat_rate_,
      SUM(ss.vat_on_gross_revenue - ss.vat_on_total_discount) AS vat,
      MAX(LAST_DAY(order_date)) AS last_day
      FROM {{ ref('shop_sales') }} ss
      WHERE product_codification NOT IN ('LOYALTY', 'LOYALTY COUPON', 'GIFT')
      GROUP BY ss.order_id, product_cod, store_code, shipping_country, year, month, vat_rate_

      UNION ALL

      SELECT ss.order_id, 'VAT' as type, 'ALL' AS product_cod, store_code, year, month,
      ss.shipping_country,
      COALESCE(tva.taux, 0.0) AS vat_rate_,
      MAX(vat_on_total_shipping) AS vat,
      MAX(LAST_DAY(order_date)) AS last_day
      FROM {{ ref('shop_sales') }} ss
      LEFT JOIN {{ ref('tva_product') }} tva ON ss.dw_country_code = tva.dw_country_code AND tva.country_code = ss.shipping_country AND tva.category = 'normal'
      GROUP BY ss.order_id, product_cod, store_code, shipping_country, year, month, tva.taux
  ) t
  GROUP BY t.type, product_codification, t.store_code, t.shipping_country, year, month, t.vat_rate_, last_day
),
decomposed_data AS (
  SELECT *
  FROM sales
  UNION ALL
  SELECT *
  FROM discount_wout_loyalty
  UNION ALL
  SELECT *
  FROM discount_loyalty
  UNION ALL
  SELECT *
  FROM shipping_shop
  UNION ALL
  SELECT *
  FROM total_vat
),
maybe_inverted_debit_credit AS (
  SELECT t1.journal,
         t1.date,
         t1.type,
         t1.p_codification,
         t1.store_code,
         t1.shipping_country,
         t1.shipping_country_classification,
         t1.year,
         t1.month,
         t1.vat_rate,
         SUM(t1.value) AS v,
         CAST(daa.account AS STRING) AS account,
         CONCAT(daa.type_nice_name, ' ', 
         CASE WHEN t1.p_codification = 'ESHOP' THEN 'FS' 
              WHEN t1.p_codification = 'SPLENDIST' THEN 'SPL'
              WHEN t1.p_codification = 'CALENDAR' THEN 'CLD'
              ELSE t1.p_codification END 
         , ' ',
         t1.shipping_country,
         CASE WHEN t1.p_codification = 'ESHOP' AND t1.shipping_country_classification <> 'HUE' AND t1.type IN ('SALES',  'DISCOUNT_WOUT_PTS', 'POINTS_DISCOUNT', 'VAT')
         THEN CONCAT(' ', t1.vat_rate, '% ') ELSE ' ' END, 
         LPAD(CAST(t1.month AS STRING), 2, '0'), RIGHT(CAST(t1.year AS STRING), 2)) AS ecriture,
         CASE WHEN t1.type IN ('DISCOUNT_WOUT_PTS', 'POINTS_DISCOUNT') THEN SUM(t1.value) ELSE 0 END AS debit,
         CASE WHEN t1.type IN ('SALES', 'SHIPPING', 'VAT') THEN SUM(t1.value) ELSE 0 END AS credit,
         daa.analytic
  FROM (
      SELECT 
      'VT1' AS journal,
      dd.last_day AS date,
      dd.type, 
      dd.product_codification AS p_codification,
      dd.store_code,
      dd.shipping_country,
      CASE 
           WHEN dd.store_code = 'Store' THEN 'FR'
           WHEN dd.shipping_country = dd.store_code THEN dd.store_code
           WHEN eu.country_code IS NOT NULL THEN 'EU'
           ELSE 'HUE'
      END AS shipping_country_classification,
      dd.year,
      dd.month,
      dd.vat_rate,
      dd.value
      FROM decomposed_data dd
      LEFT JOIN bdd_prod_fr.da_eu_countries eu ON dd.shipping_country = eu.country_code
  ) t1
  LEFT JOIN accounting.model daa 
    ON  daa.type = t1.type
    AND daa.product_codification = t1.p_codification
    AND daa.store_code = t1.store_code
    AND (t1.shipping_country_classification <> 'HUE' AND daa.shipping_country = t1.shipping_country
         OR t1.shipping_country_classification = 'HUE' AND daa.shipping_country = t1.shipping_country_classification)
    AND (daa.vat_rate = CAST(t1.vat_rate AS STRING) OR (daa.vat_rate IS NULL AND t1.vat_rate IS NULL))
  GROUP BY journal,
           date,
           t1.type,
           t1.p_codification,
           t1.store_code,
           t1.shipping_country,
           t1.shipping_country_classification,
           t1.year,
           t1.month,
           t1.vat_rate,
           ecriture,
           analytic,
           daa.account
  HAVING v <> 0
)
SELECT * EXCEPT(debit, credit),
       GREATEST(debit, -credit) AS debit,
       GREATEST(credit, -debit) AS credit
FROM maybe_inverted_debit_credit
