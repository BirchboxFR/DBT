{% set check_deleted_fr = adapter.get_columns_in_relation(source('bdd_prod_fr', 'da_eu_countries')) %}
{% set check_deleted_de = adapter.get_columns_in_relation(source('bdd_prod_de', 'da_eu_countries')) %}
{% set check_deleted_es = adapter.get_columns_in_relation(source('bdd_prod_es', 'da_eu_countries')) %}
{% set check_deleted_it = adapter.get_columns_in_relation(source('bdd_prod_it', 'da_eu_countries')) %}

SELECT 
 'FR' AS dw_country_code,
 t.*,
 {% if '__deleted' in check_deleted_fr | map(attribute='name') %}t.__deleted{% else %}false{% endif %} as is_deleted
FROM `bdd_prod_fr.da_eu_countries` t
UNION ALL 
SELECT 
 'DE' AS dw_country_code,
 t.*,
 {% if '__deleted' in check_deleted_de | map(attribute='name') %}t.__deleted{% else %}false{% endif %} as is_deleted
FROM `bdd_prod_de.da_eu_countries` t
UNION ALL
SELECT 
 'ES' AS dw_country_code,
 t.*,
 {% if '__deleted' in check_deleted_es | map(attribute='name') %}t.__deleted{% else %}false{% endif %} as is_deleted
FROM `bdd_prod_es.da_eu_countries` t
UNION ALL
SELECT 
 'IT' AS dw_country_code,
 t.*,
 {% if '__deleted' in check_deleted_it | map(attribute='name') %}t.__deleted{% else %}false{% endif %} as is_deleted
FROM `bdd_prod_it.da_eu_countries` t