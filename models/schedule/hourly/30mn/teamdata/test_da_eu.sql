{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='da_eu_countries')) -%}

SELECT 'FR' AS dw_country_code,
t.* EXCEPT(
  {% if 'deleted' in fr_columns | map(attribute='name') %}deleted,{% endif %}
  {% if 'ts_ms' in fr_columns | map(attribute='name') %}ts_ms,{% endif %}
  {% if 'transaction_order' in fr_columns | map(attribute='name') %}transaction_order,{% endif %}
  {% if 'transaction_id' in fr_columns | map(attribute='name') %}transaction_id,{% endif %}
  {% if '_rivery_river_id' in fr_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
  {% if '_rivery_run_id' in fr_columns | map(attribute='name') %}_rivery_run_id,{% endif %}
  {% if '_rivery_last_update' in fr_columns | map(attribute='name') %}_rivery_last_update{% endif %}
) 
FROM `bdd_prod_fr.da_eu_countries` t
WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}