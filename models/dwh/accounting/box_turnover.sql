WITH all_box_sales AS (
  SELECT EXTRACT(YEAR FROM payment_date) AS year_payment_date, EXTRACT(MONTH FROM payment_date) AS month_payment_date, DATE(payment_date) AS payment_date, box_id, store_code, shipping_country, gift, vat_rate, payment_status, self, year, month, date, gross_revenue, vat_on_gross_revenue, discount, vat_on_discount, shipping, vat_on_shipping
  FROM {{ ref('box_sales') }}
  WHERE self = 1
  AND payment_status = 'paid'
  UNION ALL
  SELECT EXTRACT(YEAR FROM ana.eventDate) AS year_payment_date, EXTRACT(MONTH FROM ana.eventDate) AS month_payment_date, DATE(ana.eventDate) AS payment_date, br.box_id, br.store_code, br.shipping_country, br.gift, br.vat_rate, br.payment_status, br.self, br.year, br.month, br.date, -br.gross_revenue, -br.vat_on_gross_revenue, -br.discount, -br.vat_on_discount, -br.shipping, -br.vat_on_shipping
  FROM {{ ref('box_refunds') }} br
  INNER JOIN {{ ref('order_detail_sub') }} s ON br.sub_id = s.id AND br.dw_country_code = s.dw_country_code
  LEFT JOIN {{ ref('box_sales') }} bs ON bs.sub_id = s.id AND br.dw_country_code = bs.dw_country_code
  LEFT JOIN {{ ref('adyen_notifications_authorization') }} ana ON ana.sub_id = br.sub_id AND ana.dw_country_code = br.dw_country_code
  WHERE bs.sub_id IS NULL
  AND br.self = 1
  AND br.payment_status = 'paid'
  UNION ALL
  SELECT EXTRACT(YEAR FROM br.payment_date) AS year_payment_date, EXTRACT(MONTH FROM br.payment_date) AS month_payment_date, DATE(br.payment_date) AS payment_date, br.box_id, br.store_code, br.shipping_country, br.gift, br.vat_rate, br.payment_status, br.self, br.year, br.month, br.date, br.gross_revenue, br.vat_on_gross_revenue, br.discount, br.vat_on_discount, br.shipping, br.vat_on_shipping
  FROM {{ ref('box_refunds') }} br
  left join (
  select * 
  from {{ ref('box_sales') }} bs 
  where  bs.sub_payment_status_id in (1,2,9)
  ) bs using(year,month,dw_country_code,sub_id,total_product)
  WHERE br.self = 1
  AND br.payment_status = 'paid' and bs.total_product is null
),
accounting_box_sales AS (
  SELECT *, year AS report_year, month AS report_month, date AS report_date
  FROM all_box_sales
  UNION ALL
  SELECT *, year_payment_date AS report_year, month_payment_date AS report_month, payment_date AS report_date
  FROM all_box_sales
  WHERE year_payment_date != year
  OR month_payment_date != month
)
SELECT t.*, 
CASE WHEN payment_period = '02- current_month' AND box = '02- current_box' THEN 'SELF BOX current month/current box'
     WHEN payment_period = '01- past' AND box = '02- current_box' THEN 'SELF BOX past month/current box'
     WHEN payment_period = '02- current_month' AND box = '03- future_box = PCA' THEN 'SELF BOX current month/future box'
     WHEN payment_period = '02- current_month' AND box = '01- previous_box' THEN 'SELF BOX current month/previous box'
     ELSE payment_period || ' - ' || box END AS product_codification
FROM
(
    SELECT bs.report_year,
           bs.report_month,
           CASE WHEN bs.payment_date < bs.report_date THEN '01- past'
                WHEN bs.payment_date > LAST_DAY(bs.report_date) THEN '03- future_payments'
                ELSE '02- current_month'
           END AS payment_period,
           CASE WHEN bs.date < DATE_TRUNC(bs.report_date, MONTH) THEN '01- previous_box'
                WHEN bs.date > DATE_TRUNC(bs.report_date, MONTH) THEN '03- future_box = PCA'
                WHEN bs.date = DATE_TRUNC(bs.report_date, MONTH) THEN '02- current_box'
                ELSE 'other'
           END AS box,
           bs.store_code,
           bs.shipping_country,
           CASE WHEN bs.shipping_country = bs.store_code THEN bs.store_code
               WHEN MAX(eu.country_code) IS NOT NULL THEN 'EU'
               ELSE 'HUE'
           END AS shipping_country_classification,
           CASE WHEN bs.gift = 1 THEN 'GIFT' ELSE 'SELF' END AS sub_category,
           bs.vat_rate,
           SUM(gross_revenue) AS gross_revenue,
           SUM(vat_on_gross_revenue) AS vat_on_gross_revenue,
           SUM(discount) AS discount,
           SUM(vat_on_discount) AS vat_on_discount,
           SUM(shipping) AS shipping,
           SUM(vat_on_shipping) AS vat_on_shipping
    FROM accounting_box_sales bs
    LEFT JOIN bdd_prod_fr.da_eu_countries eu ON bs.shipping_country = eu.country_code and 
    GROUP BY report_year, report_month, payment_period, box, vat_rate, bs.gift, bs.self, bs.store_code, bs.shipping_country
) t
WHERE payment_period  <> '03- future_payments'
