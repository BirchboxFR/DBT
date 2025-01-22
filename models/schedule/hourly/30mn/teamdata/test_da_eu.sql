{%- set columns = adapter.get_columns_in_relation(this) -%}

SELECT 'FR' AS dw_country_code,
t.* EXCEPT(
  {% if '__deleted' in columns | map(attribute='name') %}__deleted,{% endif %}
  {% if '__ts_ms' in columns | map(attribute='name') %}__ts_ms,{% endif %}
  {% if '__transaction_order' in columns | map(attribute='name') %}__transaction_order,{% endif %}
  {% if '__transaction_id' in columns | map(attribute='name') %}__transaction_id,{% endif %}
  {% if '_rivery_river_id' in columns | map(attribute='name') %}_rivery_river_id,{% endif %}
  {% if '_rivery_run_id' in columns | map(attribute='name') %}_rivery_run_id,{% endif %}
  {% if '_rivery_last_update' in columns | map(attribute='name') %}_rivery_last_update{% endif %}
)
FROM `bdd_prod_fr.da_eu_countries` t
WHERE {% if '__deleted' in columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}
UNION ALL 
SELECT 'DE' AS dw_country_code, 
t.* EXCEPT(
  {% if '__deleted' in columns | map(attribute='name') %}__deleted,{% endif %}
  {% if '__ts_ms' in columns | map(attribute='name') %}__ts_ms,{% endif %}
  {% if '__transaction_order' in columns | map(attribute='name') %}__transaction_order,{% endif %}
  {% if '__transaction_id' in columns | map(attribute='name') %}__transaction_id,{% endif %}
  {% if '_rivery_river_id' in columns | map(attribute='name') %}_rivery_river_id,{% endif %}
  {% if '_rivery_run_id' in columns | map(attribute='name') %}_rivery_run_id,{% endif %}
  {% if '_rivery_last_update' in columns | map(attribute='name') %}_rivery_last_update{% endif %}
) 
FROM `bdd_prod_de.da_eu_countries` t
WHERE {% if '__deleted' in columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}
UNION ALL
SELECT 'ES' AS dw_country_code, 
t.* EXCEPT(
  {% if '__deleted' in columns | map(attribute='name') %}__deleted,{% endif %}
  {% if '__ts_ms' in columns | map(attribute='name') %}__ts_ms,{% endif %}
  {% if '__transaction_order' in columns | map(attribute='name') %}__transaction_order,{% endif %}
  {% if '__transaction_id' in columns | map(attribute='name') %}__transaction_id,{% endif %}
  {% if '_rivery_river_id' in columns | map(attribute='name') %}_rivery_river_id,{% endif %}
  {% if '_rivery_run_id' in columns | map(attribute='name') %}_rivery_run_id,{% endif %}
  {% if '_rivery_last_update' in columns | map(attribute='name') %}_rivery_last_update{% endif %}
)
FROM `bdd_prod_es.da_eu_countries` t
WHERE {% if '__deleted' in columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}
UNION ALL
SELECT 'IT' AS dw_country_code, 
t.* EXCEPT(
  {% if '__deleted' in columns | map(attribute='name') %}__deleted,{% endif %}
  {% if '__ts_ms' in columns | map(attribute='name') %}__ts_ms,{% endif %}
  {% if '__transaction_order' in columns | map(attribute='name') %}__transaction_order,{% endif %}
  {% if '__transaction_id' in columns | map(attribute='name') %}__transaction_id,{% endif %}
  {% if '_rivery_river_id' in columns | map(attribute='name') %}_rivery_river_id,{% endif %}
  {% if '_rivery_run_id' in columns | map(attribute='name') %}_rivery_run_id,{% endif %}
  {% if '_rivery_last_update' in columns | map(attribute='name') %}_rivery_last_update{% endif %}
)
FROM `bdd_prod_it.da_eu_countries` t
WHERE {% if '__deleted' in columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}