{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='da_eu_countries')) -%}

SELECT 'FR' AS dw_country_code,
t.* 
FROM `bdd_prod_fr.da_eu_countries` t
WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}