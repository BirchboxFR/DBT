{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_tva_product')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_tva_product')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_tva_product')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_tva_product')) -%}

SELECT 'FR' AS dw_country_code,
country_code,category,max(taux) taux
FROM `bdd_prod_fr.wp_jb_tva_product` t
WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}
AND category='normal'
group by all

UNION ALL

SELECT 'DE' AS dw_country_code,
country_code,category,max(taux) taux
FROM `bdd_prod_de.wp_jb_tva_product` t
WHERE {% if '__deleted' in de_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}
AND category='normal'
group by all

UNION ALL

SELECT 'ES' AS dw_country_code,
country_code,category,max(taux) taux
FROM `bdd_prod_es.wp_jb_tva_product` t
WHERE {% if '__deleted' in es_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}
AND category='normal'
group by all

UNION ALL

SELECT 'IT' AS dw_country_code,
country_code,category,max(taux) taux
FROM `bdd_prod_it.wp_jb_tva_product` t
WHERE {% if '__deleted' in it_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}
AND category='normal'
group by all