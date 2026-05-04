DECLARE p_month string ;
DECLARE p_year INT64 ;
set p_month='02';
set p_year=26;

SELECT  FORMAT_DATE('%d %m %Y',  date) as date,'VT' as journal, account, -- Modification 1: Enlever les taux de TVA et remplacer ALL par ESHOP/STORE selon le store_code
      concat(store_code,' ', REGEXP_REPLACE(
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
       )) AS ecriture,
     concat(cast(p_month as string),p_year,'ESHOP',store_code,'-',shipping_country_classification) as numero_piece,
     

 SUM(debit) AS debit, SUM(credit) AS credit,
famille as famille_de_categorie,categorie,
 analytic
FROM (
     SELECT * EXCEPT(year, month)
      FROM accounting.shop_detailed
      WHERE year = 2026
      AND month = 02
      UNION ALL
      (SELECT journal,
              LAST_DAY(DATE(CONCAT(CAST(2026 AS STRING), '-', LPAD(CAST(2 AS STRING), 2, '0'), '-01'))) AS date,
              'TOTAL' AS type,
              'TOTAL' AS p_codification,
              store_code,
              NULL AS shipping_country,
              shipping_country_classification,
              NULL AS vat_rate,
              NULL AS v,
              CASE WHEN store_code IN ('FR', 'DE', 'ES', 'IT') THEN '411ESHOP' ELSE '411STORE' END AS account,
              CONCAT('Sales ', CASE WHEN store_code IN ('FR', 'DE', 'ES', 'IT') THEN CONCAT('Eshop ', shipping_country_classification)
                               WHEN store_code = 'Store' THEN 'Store '
                          END
              , LPAD(CAST(2 AS STRING), 2, '0'), RIGHT(CAST(2026 AS STRING), 2)) AS ecriture,
              NULL AS analytic,
              SUM(credit - debit) AS debit,
              NULL AS credit
      FROM accounting.shop_detailed
 --     WHERE p_codification <> 'DONATION'
      WHERE year = 2026
      AND month = 2
      GROUP BY journal, store_code, shipping_country_classification))

left join `teamdata-291012.accounting.analytics` ana on ana.code=analytic
GROUP BY journal, store_code, date, shipping_country_classification, account, analytic, famille, categorie
--having account is not null
ORDER BY CASE WHEN store_code = 'FR' THEN 1
              WHEN store_code = 'DE' THEN 2
              WHEN store_code = 'ES' THEN 3
              WHEN store_code = 'IT' THEN 4
              WHEN store_code = 'Store' THEN 5
         END,
         CASE WHEN shipping_country_classification = 'FR' THEN 1
              WHEN shipping_country_classification = 'DE' THEN 2
              WHEN shipping_country_classification = 'ES' THEN 3
              WHEN shipping_country_classification = 'IT' THEN 4
              WHEN shipping_country_classification = 'EU' THEN 5
              WHEN shipping_country_classification = 'HUE' THEN 6
         END,
         CASE WHEN MAX(p_codification) = 'BYOB' THEN 1
              WHEN MAX(p_codification) = 'CALENDAR' THEN 2
              WHEN MAX(p_codification) = 'ESHOP' THEN 3
              WHEN MAX(p_codification) = 'LTE' THEN 4
              WHEN MAX(p_codification) = 'SPLENDIST' THEN 5
              WHEN MAX(p_codification) = 'ALL' AND MAX(type) = 'VAT' THEN 6
              WHEN MAX(p_codification) = 'TOTAL' THEN 7
         END,
         account,
         CASE WHEN MAX(type) = 'SALES' THEN 1
              WHEN MAX(type) = 'DISCOUNT_WOUT_PTS' THEN 2
              WHEN MAX(type) = 'POINTS_DISCOUNT' THEN 3
              WHEN MAX(type) = 'SHIPPING' THEN 4
         END