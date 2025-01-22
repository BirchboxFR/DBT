SELECT 'DE' AS dw_country_code, id,user_id,order_id,sub_id,payment_gateway_id,transaction_id,amount,payment_profile_id,auto,status_id, payment_method_id,data,date(date)date,created_at,updated_at  FROM `bdd_prod_de.wp_jb_payments`
UNION ALL
SELECT 'ES' AS dw_country_code, id,user_id,order_id,sub_id,payment_gateway_id,transaction_id,amount,payment_profile_id,auto,status_id, payment_method_id,data,date(date)date,created_at,updated_at  FROM `bdd_prod_es.wp_jb_payments`
UNION ALL
SELECT 'IT' AS dw_country_code, id,user_id,order_id,sub_id,payment_gateway_id,transaction_id,amount,payment_profile_id,auto,status_id, payment_method_id,data,date(date)date,created_at,updated_at  FROM `bdd_prod_it.wp_jb_payments`
union all
SELECT 'FR' AS dw_country_code, id,user_id,order_id,sub_id,payment_gateway_id,transaction_id,amount,payment_profile_id,auto,status_id, payment_method_id,data,date(date)date,created_at,updated_at  FROM `bdd_prod_fr.wp_jb_payments`
