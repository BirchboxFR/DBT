{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='da_eu_countries')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='da_eu_countries')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='da_eu_countries')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='da_eu_countries')) -%}

SELECT 'FR' AS dw_country_code,
country_code,category,max(taux) taux
FROM `bdd_prod_fr.da_eu_countries` t
WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}
AND category='normal'
group by all

UNION ALL

SELECT 'DE' AS dw_country_code,
country_code,category,max(taux) taux
FROM `bdd_prod_de.da_eu_countries` t
WHERE {% if '__deleted' in de_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}
AND category='normal'
group by all

UNION ALL

SELECT 'ES' AS dw_country_code,
country_code,category,max(taux) taux
FROM `bdd_prod_es.da_eu_countries` t
WHERE {% if '__deleted' in es_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}
AND category='normal'
group by all

UNION ALL

SELECT 'IT' AS dw_country_code,
country_code,category,max(taux) taux
FROM `bdd_prod_it.da_eu_countries` t
WHERE {% if '__deleted' in it_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}
AND category='normal'
group by all