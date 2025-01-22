SELECT 'FR' AS dw_country_code,
t.* {% if '__deleted' in columns | map(attribute='name') and '__ts_ms' in columns | map(attribute='name') and '__transaction_order' in columns | map(attribute='name') and '__transaction_id' in columns | map(attribute='name') and '__rivery_river_id' in columns | map(attribute='name') and '__rivery_run_id' in columns | map(attribute='name') and '__rivery_last_update' in columns | map(attribute='name')%}EXCEPT(__deleted, __ts_ms, __transaction_order, __transaction_id, _rivery_river_id, _rivery_run_id, _rivery_last_update){% endif %}
FROM `bdd_prod_fr.da_eu_countries` t
WHERE {% if '__deleted' in columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}
UNION ALL 
SELECT 'DE' AS dw_country_code, 
t.* {% if '__deleted' in columns | map(attribute='name') and '__ts_ms' in columns | map(attribute='name') and '__transaction_order' in columns | map(attribute='name') and '__transaction_id' in columns | map(attribute='name') and '__rivery_river_id' in columns | map(attribute='name') and '__rivery_run_id' in columns | map(attribute='name') and '__rivery_last_update' in columns | map(attribute='name')%}EXCEPT(__deleted, __ts_ms, __transaction_order, __transaction_id, _rivery_river_id, _rivery_run_id, _rivery_last_update){% endif %}

FROM `bdd_prod_de.da_eu_countries` t
WHERE {% if '__deleted' in columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}
UNION ALL
SELECT 'ES' AS dw_country_code, 
t.* {% if '__deleted' in columns | map(attribute='name') and '__ts_ms' in columns | map(attribute='name') and '__transaction_order' in columns | map(attribute='name') and '__transaction_id' in columns | map(attribute='name') and '__rivery_river_id' in columns | map(attribute='name') and '__rivery_run_id' in columns | map(attribute='name') and '__rivery_last_update' in columns | map(attribute='name')%}EXCEPT(__deleted, __ts_ms, __transaction_order, __transaction_id, _rivery_river_id, _rivery_run_id, _rivery_last_update){% endif %}

FROM `bdd_prod_es.da_eu_countries` t
WHERE {% if '__deleted' in columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}
UNION ALL
SELECT 'IT' AS dw_country_code, 
t.* {% if '__deleted' in columns | map(attribute='name') and '__ts_ms' in columns | map(attribute='name') and '__transaction_order' in columns | map(attribute='name') and '__transaction_id' in columns | map(attribute='name') and '__rivery_river_id' in columns | map(attribute='name') and '__rivery_run_id' in columns | map(attribute='name') and '__rivery_last_update' in columns | map(attribute='name')%}EXCEPT(__deleted, __ts_ms, __transaction_order, __transaction_id, _rivery_river_id, _rivery_run_id, _rivery_last_update){% endif %}

FROM `bdd_prod_it.da_eu_countries` t
WHERE {% if '__deleted' in columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}