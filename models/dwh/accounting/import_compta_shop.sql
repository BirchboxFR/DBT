WITH params AS (
  SELECT
    '02' AS p_month,
    26 AS p_year,
    2026 AS p_year_full,
    2 AS p_month_int,
    DATE(2026, 2, 1) AS first_day,
    LAST_DAY(DATE(2026, 2, 1), MONTH) AS last_day
),

base AS (
  SELECT
    sd.journal,
    sd.date,
    sd.type,
    sd.p_codification,
    sd.store_code,
    sd.shipping_country,
    sd.shipping_country_classification,
    sd.vat_rate,
    sd.v,
    sd.account,
    sd.ecriture,
    sd.analytic,
    sd.debit,
    sd.credit,
    ana.famille,
    ana.categorie
  FROM accounting.shop_detailed sd
  LEFT JOIN `teamdata-291012.accounting.analytics` ana
    ON ana.code = sd.analytic
    AND STARTS_WITH(sd.account, '7')
  WHERE sd.year = 2026
    AND sd.month = 2

  UNION ALL

  SELECT
    journal,
    LAST_DAY(DATE(2026, 2, 1), MONTH) AS date,
    'TOTAL' AS type,
    'TOTAL' AS p_codification,
    store_code,
    CAST(NULL AS STRING) AS shipping_country,
    shipping_country_classification,
    CAST(NULL AS NUMERIC) AS vat_rate,
    CAST(NULL AS FLOAT64) AS v,
    CASE WHEN store_code IN ('FR', 'DE', 'ES', 'IT') THEN '411ESHOP' ELSE '411STORE' END AS account,
    CONCAT(
      'Sales ',
      CASE
        WHEN store_code IN ('FR', 'DE', 'ES', 'IT') THEN CONCAT('Eshop ', shipping_country_classification)
        WHEN store_code = 'Store' THEN 'Store '
      END,
      ' 0226'
    ) AS ecriture,
    CAST(NULL AS STRING) AS analytic,
    SUM(credit - debit) AS debit,
    CAST(NULL AS FLOAT64) AS credit,
    CAST(NULL AS STRING) AS famille,
    CAST(NULL AS STRING) AS categorie
  FROM accounting.shop_detailed
  WHERE year = 2026
    AND month = 2
  GROUP BY journal, store_code, shipping_country_classification
)

SELECT
  FORMAT_DATE('%d %m %Y', date) AS date,
  'VT' AS journal,
  account,
  CONCAT('0226ESHOP', store_code, '-', shipping_country_classification) AS numero_piece,
  CONCAT(
    store_code, ' ',
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          MAX(ecriture),
          r'[A-Z]{2,3}(( \d{4}))',
          CONCAT(shipping_country_classification, '\\1')
        ),
        r' (\d+(?:\.\d+)?% )',
        ' '
      ),
      r' ALL ',
      CASE WHEN store_code = 'Store' THEN ' STORE ' ELSE ' ESHOP ' END
    )
  ) AS ecriture,
  SUM(debit) AS debit,
  SUM(credit) AS credit,
  famille AS famille_de_categorie,
  categorie,
  analytic
FROM base
GROUP BY journal, store_code, date, shipping_country_classification, account, analytic, famille, categorie
ORDER BY
  CASE store_code
    WHEN 'FR' THEN 1 WHEN 'DE' THEN 2 WHEN 'ES' THEN 3
    WHEN 'IT' THEN 4 WHEN 'Store' THEN 5
  END,
  CASE shipping_country_classification
    WHEN 'FR' THEN 1 WHEN 'DE' THEN 2 WHEN 'ES' THEN 3
    WHEN 'IT' THEN 4 WHEN 'EU' THEN 5 WHEN 'HUE' THEN 6
  END,
  CASE
    WHEN STARTS_WITH(account, '701') THEN 1
    WHEN STARTS_WITH(account, '707') THEN 2
    WHEN STARTS_WITH(account, '708') THEN 3
    WHEN STARTS_WITH(account, '709') THEN 4
    WHEN STARTS_WITH(account, '4457') THEN 5
    WHEN STARTS_WITH(account, '411') THEN 6
    ELSE 7
  END,
  CASE MAX(p_codification)
    WHEN 'BYOB' THEN 1 WHEN 'CALENDAR' THEN 2 WHEN 'ESHOP' THEN 3
    WHEN 'LTE' THEN 4 WHEN 'SPLENDIST' THEN 5 WHEN 'TOTAL' THEN 7
  END,
  account,
  CASE MAX(type)
    WHEN 'SALES' THEN 1 WHEN 'DISCOUNT_WOUT_PTS' THEN 2
    WHEN 'POINTS_DISCOUNT' THEN 3 WHEN 'SHIPPING' THEN 4
  END