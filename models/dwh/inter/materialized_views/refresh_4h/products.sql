

{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_products')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_products')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_products')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_products')) -%}


SELECT 'FR' AS dw_country_code, t.*except(sku,attr_special_price_start,attr_special_price_end,attr_free_shipping_start,attr_free_shipping_end, attr_shipping_delayed_to,
{% if '__deleted' in fr_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in fr_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in fr_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in fr_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in fr_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in fr_columns | map(attribute='name') %}_rivery_run_id,{% endif %}
 {% if '_rivery_last_update' in fr_columns | map(attribute='name') %}_rivery_last_update{% endif %}),
 case when sku='' then null else sku end as sku,
safe_cast(attr_special_price_start as date) as attr_special_price_start,
safe_cast(attr_special_price_end as date) as attr_special_price_end,
safe_cast(attr_free_shipping_start as date) as attr_free_shipping_start,
safe_cast(attr_free_shipping_end as date) as attr_free_shipping_end,
safe_cast(attr_shipping_delayed_to as date) as attr_shipping_delayed_to
 FROM `bdd_prod_fr.wp_jb_products` t
 WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false){% else %}true{% endif %}

UNION ALL 
SELECT 'DE' AS dw_country_code,  t.*except(sku,attr_special_price_start,attr_special_price_end,attr_free_shipping_start,attr_free_shipping_end, attr_shipping_delayed_to,
{% if '__deleted' in de_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in de_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in de_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in de_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in de_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in de_columns | map(attribute='name') %}_rivery_run_id,{% endif %}
 {% if '_rivery_last_update' in de_columns | map(attribute='name') %}_rivery_last_update{% endif %}),
 case when sku='' then null else sku end as sku,
safe_cast(attr_special_price_start as date) as attr_special_price_start,
safe_cast(attr_special_price_end as date) as attr_special_price_end,
safe_cast(attr_free_shipping_start as date) as attr_free_shipping_start,
safe_cast(attr_free_shipping_end as date) as attr_free_shipping_end,
safe_cast(attr_shipping_delayed_to as date) as attr_shipping_delayed_to
 FROM `bdd_prod_de.wp_jb_products` t
 WHERE {% if '__deleted' in de_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false){% else %}true{% endif %}

UNION ALL 
SELECT 'ES' AS dw_country_code,  t.*except(sku,attr_special_price_start,attr_special_price_end,attr_free_shipping_start,attr_free_shipping_end, attr_shipping_delayed_to,
{% if '__deleted' in es_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in es_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in es_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in es_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in es_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in es_columns | map(attribute='name') %}_rivery_run_id,{% endif %}
 {% if '_rivery_last_update' in es_columns | map(attribute='name') %}_rivery_last_update{% endif %}),
 case when sku='' then null else sku end as sku,
safe_cast(attr_special_price_start as date) as attr_special_price_start,
safe_cast(attr_special_price_end as date) as attr_special_price_end,
safe_cast(attr_free_shipping_start as date) as attr_free_shipping_start,
safe_cast(attr_free_shipping_end as date) as attr_free_shipping_end,
safe_cast(attr_shipping_delayed_to as date) as attr_shipping_delayed_to
 FROM `bdd_prod_es.wp_jb_products` t
 WHERE {% if '__deleted' in es_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}

UNION ALL 
SELECT 'IT' AS dw_country_code,  t.*except(sku,attr_special_price_start,attr_special_price_end,attr_free_shipping_start,attr_free_shipping_end, attr_shipping_delayed_to,
{% if '__deleted' in it_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in it_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in it_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in it_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in it_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in it_columns | map(attribute='name') %}_rivery_run_id,{% endif %}
 {% if '_rivery_last_update' in it_columns | map(attribute='name') %}_rivery_last_update{% endif %}),
 case when sku='' then null else sku end as sku,
safe_cast(attr_special_price_start as date) as attr_special_price_start,
safe_cast(attr_special_price_end as date) as attr_special_price_end,
safe_cast(attr_free_shipping_start as date) as attr_free_shipping_start,
safe_cast(attr_free_shipping_end as date) as attr_free_shipping_end,
safe_cast(attr_shipping_delayed_to as date) as attr_shipping_delayed_to 
FROM `bdd_prod_it.wp_jb_products` t

WHERE {% if '__deleted' in it_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}
