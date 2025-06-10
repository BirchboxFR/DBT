{{ config(
    partition_by={
      "field": "id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 50000000,
        "interval": 30000
      }
    },
    cluster_by=['dw_country_code', 'billing_city']
) }}



{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='_prod_fr', identifier='wp_jb_orders')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_orders')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_orders')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_orders')) -%}


-- heures pour check
{%- set lookback_hours = 4 -%}

-- Sélection des données françaises
SELECT 'FR' AS dw_country_code,
ID,	user_id,	subscriber_id,	total,	status_id,	billing_civility,	billing_firstname,	billing_lastname,	billing_adr1,
	billing_adr2,	billing_adr3,	billing_adr4,	billing_zipcode,	billing_city,	billing_country,	billing_phone,
  	date,	modified,	shipping_status_id,	shipping_civility,	shipping_firstname,	shipping_lastname,
    	shipping_adr1,	shipping_adr2,	shipping_adr3,	shipping_adr4,	shipping_zipcode,
      	shipping_city,	shipping_country,	shipping_phone,	shipping_mode,	total_shipping,	total_discount,	total_points_discount,
        	total_coupon_discount,	total_products_discount,	total_store_discount,	total_points_used,	total_points_gain,
          	total_sub_discount,	coupon_code_id,	raf_parent_id,	order_parent_id,	store_id,	mondial_relay_code,	
            dhl_packstation_number,	gift_message,	is_active_sub,	is_first_order,	is_first_shop_order,	is_first_sub_order,
            	fraud_check_status,safe_cast(	estimated_delivery_date as timestamp) as estimated_delivery_date,	created_at,	updated_at,_airbyte_extracted_at as _rivery_last_update
FROM `prod_fr.wp_jb_orders` t
WHERE 
  -- Filtre sur les lignes non supprimées (CDC)
  t._ab_cdc_deleted_at IS NULL 
  -- Filtre sur les données récentes uniquement
  {% if is_incremental() %}
  AND timestamp(t._ab_cdc_updated_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
  {% endif %}
UNION ALL

SELECT 'DE' AS dw_country_code,
t.* EXCEPT(
 {% if '__deleted' in de_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in de_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in de_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in de_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in de_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in de_columns | map(attribute='name') %}_rivery_run_id{% endif %}
 --{% if '_rivery_last_update' in de_columns | map(attribute='name') %}_rivery_last_update{% endif %}
) 
FROM `bdd_prod_de.wp_jb_orders` t
WHERE 
  {% if '__deleted' in de_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) AND{% endif %}
  {% if is_incremental() %}
  (
    t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
    OR t.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
  )
  {% else %}
  TRUE
  {% endif %}

UNION ALL

SELECT 'ES' AS dw_country_code,
t.* EXCEPT(
 {% if '__deleted' in es_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in es_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in es_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in es_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in es_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in es_columns | map(attribute='name') %}_rivery_run_id{% endif %}
 --{% if '_rivery_last_update' in es_columns | map(attribute='name') %}_rivery_last_update{% endif %}
) 
FROM `bdd_prod_es.wp_jb_orders` t
WHERE 
  {% if '__deleted' in es_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) AND{% endif %}
  {% if is_incremental() %}
  (
    t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
    OR t.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
  )
  {% else %}
  TRUE
  {% endif %}

UNION ALL

SELECT 'IT' AS dw_country_code,
t.* EXCEPT(
 {% if '__deleted' in it_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in it_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in it_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in it_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in it_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in it_columns | map(attribute='name') %}_rivery_run_id{% endif %}
 --{% if '_rivery_last_update' in it_columns | map(attribute='name') %}_rivery_last_update{% endif %}
) 
FROM `bdd_prod_it.wp_jb_orders` t
WHERE 
  {% if '__deleted' in it_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) AND{% endif %}
  {% if is_incremental() %}
  (
    t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
    OR t.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
  )
  {% else %}
  TRUE
  {% endif %}