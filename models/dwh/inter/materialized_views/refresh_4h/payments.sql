{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_payments')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_payments')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_payments')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_payments')) -%}

SELECT 'FR' AS dw_country_code,
id,user_id,order_id,sub_id,payment_gateway_id,transaction_id,amount,payment_profile_id,auto,status_id, payment_method_id,data,date(date)date,created_at,updated_at  
FROM `bdd_prod_fr.wp_jb_payments` t
WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}

UNION ALL

SELECT 'DE' AS dw_country_code,
id,user_id,order_id,sub_id,payment_gateway_id,transaction_id,amount,payment_profile_id,auto,status_id, payment_method_id,data,date(date)date,created_at,updated_at  
FROM `bdd_prod_de.wp_jb_payments` t
WHERE {% if '__deleted' in de_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}

UNION ALL

SELECT 'ES' AS dw_country_code,
id,user_id,order_id,sub_id,payment_gateway_id,transaction_id,amount,payment_profile_id,auto,status_id, payment_method_id,data,date(date)date,created_at,updated_at  
FROM `bdd_prod_es.wp_jb_payments` t
WHERE {% if '__deleted' in es_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}

UNION ALL

SELECT 'IT' AS dw_country_code,
id,user_id,order_id,sub_id,payment_gateway_id,transaction_id,amount,payment_profile_id,auto,status_id, payment_method_id,data,date(date)date,created_at,updated_at  
FROM `bdd_prod_it.wp_jb_payments` t
WHERE {% if '__deleted' in it_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}
