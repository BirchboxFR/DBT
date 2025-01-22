{%- set columns = adapter.get_columns_in_relation(this) -%}

SELECT 'FR' AS dw_country_code,
t.* 
FROM `bdd_prod_fr.da_eu_countries` t
WHERE {% if '__deleted' in columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}
