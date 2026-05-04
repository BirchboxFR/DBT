WITH box_detailed AS (
  SELECT * EXCEPT (report_year, report_month)
  FROM accounting.box_detailed
  WHERE report_year = 2026
    AND report_month = 2
),
params AS (
  SELECT
    DATE(report_year, report_month, 1) AS first_day,
    LAST_DAY(DATE(report_year, report_month, 1), MONTH) AS last_day
  FROM (SELECT 2026 AS report_year, 2 AS report_month)
),
base AS (
  SELECT
    CASE
      WHEN b.date = 'last day'  THEN p.last_day
      WHEN b.date = 'first day' THEN p.first_day
    END AS date,
    journal, account,
    CASE WHEN store_code IN ('FR', 'DE', 'ES', 'IT') THEN '411ESHOP' ELSE '411STORE' END AS account_,
    CASE
      WHEN ordre_ecriture = 1 THEN CONCAT('0226BOX', store_code, '-', shipping_country_classification)
      WHEN ordre_ecriture = 2 THEN CONCAT('0226PCA', store_code)
      WHEN ordre_ecriture = 3 THEN CONCAT('X0226PCA', store_code)
    END AS numero_piece,
    CASE
      WHEN ecriture LIKE '%PCA%' OR ecriture LIKE '%Ext.%' THEN ecriture
      ELSE CONCAT(store_code, ' ', ecriture)
    END AS ecriture,
    GREATEST(SUM(debit - credit), 0) AS debit,
    GREATEST(SUM(credit - debit), 0) AS credit,
    famille AS famille_de_categorie,
    categorie,
    analytic,
    store_code,
    shipping_country_classification,
    ordre_ecriture
  FROM (
    SELECT
      date, 'VT' AS journal, shipping_country_classification, code_libelle, store_code, account, analytic,
      REGEXP_REPLACE(
        REGEXP_REPLACE(ecriture, r'Extourne ', 'Ext. '),
        r'[A-Z]{2}( \d{4})',
        CONCAT(shipping_country_classification, '\\1')
      ) AS ecriture,
      CASE
        WHEN ecriture NOT LIKE '%Extourne%' AND ecriture NOT LIKE '%PCA%' THEN 1
        WHEN ecriture LIKE '%PCA%' AND ecriture NOT LIKE '%Extourne%'     THEN 2
        WHEN ecriture LIKE '%Extourne%'                                    THEN 3
      END AS ordre_ecriture,
      debit, credit
    FROM box_detailed
  ) b
  CROSS JOIN params p
  LEFT JOIN `teamdata-291012.accounting.analytics` ana ON ana.code = analytic
  GROUP BY
    journal, code_libelle, store_code, account, analytic, ecriture, date,
    shipping_country_classification, famille, categorie, ordre_ecriture  -- ordre_ecriture ajouté ici
  HAVING account IS NOT NULL
),
totals AS (
  SELECT
    MAX(date) AS date,
    'VT' AS journal,
    CASE 
      WHEN ordre_ecriture = 1 THEN '411ESHOP'
      ELSE '487100000'
    END AS account,
    CAST(NULL AS STRING) AS account_,
   CASE ordre_ecriture
  WHEN 1 THEN CONCAT('0226BOX', store_code, '-', shipping_country_classification)
  WHEN 2 THEN CONCAT('0226PCA', store_code)
  WHEN 3 THEN CONCAT('X0226PCA', store_code)
END AS numero_piece,
    CASE ordre_ecriture
      WHEN 1 THEN CONCAT(store_code, ' Sales Box ', shipping_country_classification, ' 0226')
      WHEN 2 THEN CONCAT('PCA Sales BOX ', shipping_country_classification, ' 0226')
      WHEN 3 THEN CONCAT('Ext. PCA Sales BOX ', shipping_country_classification, ' 0226')
    END AS ecriture,
    GREATEST(SUM(credit) - SUM(debit), 0) AS debit,
    GREATEST(SUM(debit) - SUM(credit), 0) AS credit,
    CAST(NULL AS STRING) AS famille_de_categorie,
    CAST(NULL AS STRING) AS categorie,
    CAST(NULL AS STRING) AS analytic,
    store_code,
    shipping_country_classification,
    ordre_ecriture
  FROM base
  GROUP BY store_code, shipping_country_classification, ordre_ecriture
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
ORDER BY
  ordre_ecriture,  -- 1=écriture, 2=PCA, 3=extourne (priorité absolue)
  CASE store_code
    WHEN 'FR' THEN 1 WHEN 'DE' THEN 2 WHEN 'ES' THEN 3
    WHEN 'IT' THEN 4 WHEN 'PL' THEN 5 WHEN 'SE' THEN 6
  END,
  CASE shipping_country_classification
    WHEN 'FR' THEN 1 WHEN 'DE' THEN 2 WHEN 'ES' THEN 3
    WHEN 'IT' THEN 4 WHEN 'PL' THEN 5 WHEN 'SE' THEN 6
    WHEN 'EU' THEN 7 WHEN 'HUE' THEN 8
  END,
  CASE
    WHEN STARTS_WITH(account, '701') THEN 1
    WHEN STARTS_WITH(account, '7085') THEN 2
    WHEN STARTS_WITH(account, '7091') THEN 3
    WHEN STARTS_WITH(account, '4457') THEN 4
    WHEN STARTS_WITH(account, '411') THEN 5
    ELSE 6
  END,
  account