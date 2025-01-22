
SELECT 'FR' AS dw_country_code,t.*except(invoice_date,order_date,payment_date), 
safe_cast(invoice_date as date) as invoice_date,
safe_cast(payment_date as date) as payment_date,
safe_cast(order_date as date) as order_date
 FROM `bdd_prod_fr.wp_jb_invoices` t
UNION ALL
SELECT 'DE' AS dw_country_code,t.*except(invoice_date,order_date,payment_date), 
safe_cast(invoice_date as date) as invoice_date,
safe_cast(payment_date as date) as payment_date,
safe_cast(order_date as date) as order_date FROM `bdd_prod_de.wp_jb_invoices` t
UNION ALL
SELECT 'ES' AS dw_country_code,t.*except(invoice_date,order_date,payment_date), 
safe_cast(invoice_date as date) as invoice_date,
safe_cast(payment_date as date) as payment_date,
safe_cast(order_date as date) as order_date FROM `bdd_prod_es.wp_jb_invoices` t
UNION ALL
SELECT 'IT' AS dw_country_code,t.*except(invoice_date,order_date,payment_date), 
safe_cast(invoice_date as date) as invoice_date,
safe_cast(payment_date as date) as payment_date,
safe_cast(order_date as date) as order_date FROM `bdd_prod_it.wp_jb_invoices` t