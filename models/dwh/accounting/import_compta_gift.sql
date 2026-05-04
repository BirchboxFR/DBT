WITH params AS (
  SELECT
    DATE(2026, 2, 1) AS first_day,
    LAST_DAY(DATE(2026, 2, 1), MONTH) AS last_day
),

data_gift AS (
  SELECT
    'GIFT' AS product_codification,
    ss.store_code,
    CAST(NULL AS STRING) AS shipping_country,
    CAST(NULL AS STRING) AS shipping_country_classification,
    SUM(ss.gross_revenue + ss.vat_on_gross_revenue) AS gross_revenue_ttc,
    SUM(ss.total_discount + ss.vat_on_total_discount) AS total_discount_ttc
  FROM sales.shop_sales ss
  WHERE ss.product_codification_id = 34
    AND ss.year = 2026
    AND ss.month = 2
  GROUP BY ss.store_code
),

green AS (
  SELECT
    'VT' AS journal,
    1 AS ordre_ecriture,
    dam.account,
    CASE WHEN dg.store_code = 'Store' THEN '411STORE' ELSE '411ESHOP' END AS account_,
    '0226GIFT' AS numero_piece,
    dam.type,
    CONCAT(dg.store_code, ' ', dam.type_nice_name, ' Gift 0226') AS ecriture,
    p.last_day AS date,
    CASE WHEN dam.type = 'SALES' THEN 0
         WHEN dam.type = 'DISCOUNT_WOUT_PTS' THEN dg.total_discount_ttc
    END AS debit,
    CASE WHEN dam.type = 'SALES' THEN dg.gross_revenue_ttc
         WHEN dam.type = 'DISCOUNT_WOUT_PTS' THEN 0
    END AS credit,
    dam.analytic,
    dg.store_code,
    dg.shipping_country_classification,
    ana.famille AS famille_de_categorie,
    ana.categorie,
    'green' AS source
  FROM data_gift dg
  INNER JOIN `teamdata-291012.accounting.model` dam
    ON dg.store_code = dam.store_code
    AND dg.product_codification = dam.product_codification
    AND dam.shipping_country IS NULL
    AND dam.type IN ('SALES', 'DISCOUNT_WOUT_PTS')
  LEFT JOIN `teamdata-291012.accounting.analytics` ana
    ON ana.code = dam.analytic
    AND STARTS_WITH(dam.account, '7')
  CROSS JOIN params p
),

blue AS (
  SELECT
    'VT' AS journal,
    2 AS ordre_ecriture,
    account,
    account_,
    '0226PCAGIFT' AS numero_piece,
    type,
    CONCAT('PCA ', ecriture) AS ecriture,
    p.first_day AS date,
    CASE WHEN credit > 0 THEN credit ELSE 0 END AS debit,
    CASE WHEN debit > 0 THEN debit ELSE 0 END AS credit,
    analytic,
    store_code,
    shipping_country_classification,
    famille_de_categorie,
    categorie,
    'blue' AS source
  FROM green
  CROSS JOIN params p
),

data_expired AS (
  SELECT
    ss.store_code,
    MAX(ss.product_codification) AS product_codification,
    SUM(ss.gross_revenue + ss.vat_on_gross_revenue) AS gross_revenue_ttc,
    SUM(ss.total_discount + ss.vat_on_total_discount) AS total_discount_ttc
  FROM sales.shop_sales ss
  INNER JOIN inter.order_details d USING(dw_country_code, order_id)
  INNER JOIN inter.gift_cards gc
    ON d.id = gc.order_detail_id
    AND d.dw_country_code = gc.dw_country_code
  WHERE ss.order_status = 'Validée'
    AND ss.product_codification_id = 34
    AND EXTRACT(YEAR FROM gc.expiration_date) = 2026
    AND EXTRACT(MONTH FROM gc.expiration_date) = 2
    AND gc.status = 'OFF'
  GROUP BY ss.store_code
),

expired_cards AS (
  SELECT
    'VT' AS journal,
    3 AS ordre_ecriture,
    dam.account,
    CASE WHEN shop.store_code = 'Store' THEN '411STORE' ELSE '411ESHOP' END AS account_,
    'X0226EXPGIFT' AS numero_piece,
    dam.type,
    CONCAT(shop.store_code, ' expired Gift 0226') AS ecriture,
    p.first_day AS date,
    CASE WHEN dam.type = 'EXPIRATION' THEN 0
         WHEN dam.type = 'DISCOUNT_EXPIRATION' THEN shop.total_discount_ttc
    END AS debit,
    CASE WHEN dam.type = 'EXPIRATION' THEN shop.gross_revenue_ttc
         WHEN dam.type = 'DISCOUNT_EXPIRATION' THEN 0
    END AS credit,
    dam.analytic,
    shop.store_code,
    CAST(NULL AS STRING) AS shipping_country_classification,
    ana.famille AS famille_de_categorie,
    ana.categorie,
    'expired' AS source
  FROM data_expired shop
  INNER JOIN `teamdata-291012.accounting.model` dam
    ON shop.store_code = dam.store_code
    AND shop.product_codification = dam.product_codification
    AND dam.shipping_country IS NULL
    AND dam.type IN ('EXPIRATION', 'DISCOUNT_EXPIRATION')
  LEFT JOIN `teamdata-291012.accounting.analytics` ana
    ON ana.code = dam.analytic
    AND STARTS_WITH(dam.account, '7')
  CROSS JOIN params p
),

data_activation AS (
  SELECT
    bs.store_code,
    bs.shipping_country,
    'GIFT' AS product_codification,
    CASE
      WHEN bs.store_code = 'Store' THEN 'FR'
      WHEN bs.shipping_country = bs.store_code THEN bs.store_code
      WHEN eu.country_code IS NOT NULL THEN 'EU'
      ELSE 'HUE'
    END AS shipping_country_classification,
    bs.vat_rate,
    SUM(bs.gross_revenue) AS gross_revenue,
    SUM(bs.discount) AS discount,
    SUM(bs.vat_on_gross_revenue - bs.vat_on_discount) AS total_vat
  FROM sales.box_sales bs
  LEFT JOIN inter.da_eu_countries eu
    ON bs.dw_country_code = eu.dw_country_code
    AND bs.shipping_country = eu.country_code
  WHERE bs.gift = 1
    AND bs.year = 2026
    AND bs.month = 2
  GROUP BY
    bs.store_code,
    bs.shipping_country,
    shipping_country_classification,
    bs.vat_rate
),

white AS (
  SELECT
    'VT' AS journal,
    3 AS ordre_ecriture,
    dam.account,
    CASE WHEN da.store_code = 'Store' THEN '411STORE' ELSE '411ESHOP' END AS account_,
    'X0226PCAGIFT' AS numero_piece,
    dam.type,
    CONCAT('Ext. PCA ', da.store_code, ' ', dam.type_nice_name, ' Gift ', da.shipping_country_classification, ' 0226') AS ecriture,
    p.first_day AS date,
    CASE WHEN dam.type = 'DISCOUNT_ACTIVATION' THEN da.discount
         WHEN dam.type IN ('ACTIVATION', 'VAT') THEN 0
    END AS debit,
    CASE WHEN dam.type = 'DISCOUNT_ACTIVATION' THEN 0
         WHEN dam.type = 'ACTIVATION' THEN da.gross_revenue
         WHEN dam.type = 'VAT' THEN da.total_vat
    END AS credit,
    dam.analytic,
    da.store_code,
    da.shipping_country_classification,
    ana.famille AS famille_de_categorie,
    ana.categorie,
    'white' AS source
  FROM data_activation da
  LEFT JOIN `teamdata-291012.accounting.model` dam
    ON da.store_code = dam.store_code
    AND (
      (
        dam.shipping_country = da.shipping_country
        OR dam.shipping_country = 'HUE' AND dam.shipping_country = da.shipping_country_classification
      )
      AND da.product_codification = dam.product_codification
      AND dam.type IN ('ACTIVATION', 'DISCOUNT_ACTIVATION')
      OR dam.product_codification = 'ALL'
      AND (
        CAST(da.vat_rate AS STRING) = dam.vat_rate
        AND dam.shipping_country = da.shipping_country
        OR dam.shipping_country IS NULL AND da.shipping_country_classification = 'HUE'
      )
    )
  LEFT JOIN `teamdata-291012.accounting.analytics` ana
    ON ana.code = dam.analytic
    AND STARTS_WITH(dam.account, '7')
  CROSS JOIN params p
),

base AS (
  SELECT * FROM green
  UNION ALL
  SELECT * FROM blue
  UNION ALL
  SELECT * FROM expired_cards
  UNION ALL
  SELECT * FROM white
),

totals AS (
  SELECT
    'VT' AS journal,
    ordre_ecriture,
    CASE
      WHEN ordre_ecriture = 1 THEN MAX(account_)
      ELSE '487200000'
    END AS account,
    CAST(NULL AS STRING) AS account_,
    CASE ordre_ecriture
      WHEN 1 THEN '0226GIFT'
      WHEN 2 THEN '0226PCAGIFT'
      WHEN 3 THEN
        CASE WHEN source = 'expired' THEN 'X0226EXPGIFT' ELSE 'X0226PCAGIFT' END
    END AS numero_piece,
    CAST(NULL AS STRING) AS type,
    CASE ordre_ecriture
      WHEN 1 THEN
        CASE WHEN store_group = 'UE' THEN 'Sales Gift UE 0226' ELSE 'Sales Gift 0226' END
      WHEN 2 THEN
        CASE WHEN store_group = 'UE' THEN 'PCA Sales Gift UE 0226' ELSE 'PCA Sales Gift 0226' END
      WHEN 3 THEN
        CASE
          WHEN source = 'expired' THEN CONCAT(store_code, ' expired Gift 0226')
          ELSE CONCAT('Ext. PCA ', store_code, ' Sales Gift 0226')
        END
    END AS ecriture,
    MAX(date) AS date,
    GREATEST(SUM(credit) - SUM(debit), 0) AS debit,
    GREATEST(SUM(debit) - SUM(credit), 0) AS credit,
    CAST(NULL AS STRING) AS analytic,
    store_code,
    CAST(NULL AS STRING) AS shipping_country_classification,
    CAST(NULL AS STRING) AS famille_de_categorie,
    CAST(NULL AS STRING) AS categorie,
    source
  FROM (
    SELECT *,
      CASE
        WHEN store_code = 'FR' THEN 'FR'
        WHEN store_code = 'Store' THEN 'Store'
        ELSE 'UE'
      END AS store_group
    FROM base
  )
  GROUP BY
    store_code,
    store_group,
    ordre_ecriture,
    source
)

SELECT
  FORMAT_DATE('%d %m %Y', date) AS date,
  journal,
  account,
  numero_piece,
  ecriture,
  debit,
  credit,
  famille_de_categorie,
  categorie,
  analytic
FROM (
  SELECT * FROM base
  UNION ALL
  SELECT * FROM totals
)
WHERE credit > 0 OR debit > 0
ORDER BY
  ordre_ecriture,
  CASE
    WHEN store_code = 'FR' THEN 1
    WHEN store_code = 'Store' THEN 3
    ELSE 2
  END,
  CASE store_code
    WHEN 'FR' THEN 1 WHEN 'DE' THEN 2 WHEN 'ES' THEN 3
    WHEN 'IT' THEN 4 WHEN 'PL' THEN 5 WHEN 'SE' THEN 6
    WHEN 'Store' THEN 7
  END,
  CASE source
    WHEN 'green' THEN 1
    WHEN 'blue' THEN 2
    WHEN 'expired' THEN 1
    WHEN 'white' THEN 2
    ELSE 3
  END,
  CASE
    WHEN STARTS_WITH(account, '707') THEN 1
    WHEN STARTS_WITH(account, '709') THEN 2
    WHEN STARTS_WITH(account, '4457') THEN 3
    WHEN STARTS_WITH(account, '411') THEN 4
    WHEN STARTS_WITH(account, '487') THEN 5
    ELSE 6
  END,
  account