SELECT 'FR' AS dw_country_code, t.* 
FROM `bdd_prod_fr.da_eu_countries` t
WHERE {% if '__deleted' in columns | map(attribute='name') %}__deleted is null{% endif %}
UNION ALL 
SELECT 'DE' AS dw_country_code, t.*
FROM `bdd_prod_de.da_eu_countries` t
WHERE {% if '__deleted' in columns | map(attribute='name') %}__deleted is null{% endif %}
UNION ALL
SELECT 'ES' AS dw_country_code, t.*
FROM `bdd_prod_es.da_eu_countries` t
WHERE {% if '__deleted' in columns | map(attribute='name') %}__deleted is null{% endif %}
UNION ALL
SELECT 'IT' AS dw_country_code, t.*
FROM `bdd_prod_it.da_eu_countries` t
WHERE {% if '__deleted' in columns | map(attribute='name') %}__deleted is null{% endif %}