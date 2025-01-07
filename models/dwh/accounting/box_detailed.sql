WITH maybe_inverted_debit_credit AS (
    SELECT t1.report_year,
           t1.report_month,
           t1.journal,
           t1.code_libelle,
           CASE WHEN t1.code_libelle = 'X' THEN 'first day'
                ELSE 'last day'
           END AS date,
           t1.type,
           t1.p_codification,
           t1.store_code,
           t1.shipping_country,
           t1.shipping_country_classification,
           t1.vat_rate,
           SUM(t1.value) AS v,
           daa.account,
           CONCAT(CASE WHEN t1.journal = 'VT1' THEN ''
                       WHEN t1.code_libelle IS NULL THEN 'PCA '
                       WHEN t1.code_libelle = 'X' THEN 'Extourne PCA '
                       ELSE ''
                  END, daa.type_nice_name, ' BOX ', t1.store_code, ' ', LPAD(CAST(report_month AS STRING), 2, '0'), RIGHT(CAST(report_year AS STRING), 2)) AS ecriture,
           CASE WHEN t1.journal = 'VT1' AND t1.type = 'DISCOUNT' THEN SUM(t1.value)
                WHEN t1.journal = 'ODS' AND t1.code_libelle IS NULL AND t1.type IN ('SALES', 'SHIPPING') THEN SUM(t1.value)
                WHEN t1.journal = 'ODS' AND t1.code_libelle = 'X' AND t1.type = 'DISCOUNT' THEN SUM(t1.value)
                ELSE 0
           END AS debit,
           CASE WHEN t1.journal = 'VT1' AND t1.type IN ('SALES', 'SHIPPING', 'VAT') THEN SUM(t1.value)
                WHEN t1.journal = 'ODS' AND t1.code_libelle IS NULL AND t1.type = 'DISCOUNT' THEN SUM(t1.value)
                WHEN t1.journal = 'ODS' AND t1.code_libelle = 'X' AND t1.type IN ('SALES', 'SHIPPING') THEN SUM(t1.value)
                ELSE 0
           END AS credit,
           daa.analytic
    FROM (
        SELECT report_year,
               report_month,
               CASE WHEN j.journal <> 'ODS Extourne' THEN j.journal
                    ELSE 'ODS'
               END AS journal,
               CASE WHEN j.journal = 'ODS Extourne' THEN 'X'
               END AS code_libelle,
               box.type,
               box.store_code,
               box.shipping_country,
               box.shipping_country_classification,
               box.product_codification AS p_codification,
               box.vat_rate,
               CASE WHEN j.journal = 'VT1' AND box.product_codification LIKE '%current month%' THEN box.value
                    WHEN j.journal = 'ODS' AND box.product_codification LIKE '%current month/future box%' THEN box.value
                    WHEN j.journal = 'ODS Extourne' AND box.product_codification LIKE '%past month/current box%' THEN box.value
                    ELSE 0
               END AS value
    FROM
    (
    -- sales
    SELECT 'SALES' AS type,
           report_year,
           report_month,
           product_codification,
           store_code,
           shipping_country,
           shipping_country_classification,
           vat_rate,
           SUM(gross_revenue) AS value
    FROM {{ ref('box_turnover') }}
    GROUP BY type,
             report_year,
             report_month,
             product_codification,
             store_code,
             shipping_country,
             shipping_country_classification,
             vat_rate

    UNION ALL

    -- discounts
    SELECT 'DISCOUNT' AS type,
           report_year,
           report_month,
           product_codification,
           store_code,
           shipping_country,
           shipping_country_classification,
           vat_rate,
           SUM(discount) AS total_discount
    FROM {{ ref('box_turnover') }}
    GROUP BY type,
             report_year,
             report_month,
             product_codification,
             store_code,
             shipping_country,
             shipping_country_classification,
             vat_rate

    UNION ALL

    -- shipping
    SELECT 'SHIPPING' AS type,
           report_year,
           report_month,
           product_codification,
           store_code,
           shipping_country,
           shipping_country_classification,
           vat_rate,
           SUM(shipping) AS total_discount
    FROM {{ ref('box_turnover') }}
    GROUP BY type,
             report_year,
             report_month,
             product_codification,
             store_code,
             shipping_country,
             shipping_country_classification,
             vat_rate

    UNION ALL

    -- VAT on sales
    SELECT 'VAT' AS type,
           report_year,
           report_month,
           product_codification,
           store_code,
           shipping_country,
           shipping_country_classification,
           vat_rate,
           SUM(vat_on_gross_revenue - vat_on_discount + vat_on_shipping) AS value
    FROM {{ ref('box_turnover') }}
    WHERE box_turnover.payment_period <> '01- past' -- pas d'extourne de TVA sur la passé
    GROUP BY type,
             report_year,
             report_month,
             product_codification,
             store_code,
             shipping_country,
             shipping_country_classification,
             vat_rate
    ) box
    CROSS JOIN accounting.journaux j
    ) t1
    LEFT JOIN accounting.model daa 
      ON  daa.type = t1.type
      AND daa.product_codification = t1.p_codification
      AND daa.store_code = t1.store_code
      AND (t1.shipping_country_classification <> 'HUE' AND daa.shipping_country = t1.shipping_country
           OR t1.shipping_country_classification = 'HUE' AND daa.shipping_country = t1.shipping_country_classification)
      AND (CAST(daa.vat_rate AS STRING) = CAST(t1.vat_rate AS STRING) OR (daa.vat_rate IS NULL AND t1.vat_rate IS NULL))
      AND t1.value <> 0

    GROUP BY report_year,
             report_month,
             journal,
             code_libelle,
             date,
             t1.type,
             t1.p_codification,
             t1.store_code,
             t1.shipping_country,
             t1.shipping_country_classification,
             t1.vat_rate,
             account,
             ecriture,
             analytic
    HAVING v <> 0
    AND (credit <> 0 OR debit <> 0)
)
SELECT * EXCEPT(debit, credit),
       GREATEST(debit, -credit) AS debit,
       GREATEST(credit, -debit) AS credit
FROM maybe_inverted_debit_credit
