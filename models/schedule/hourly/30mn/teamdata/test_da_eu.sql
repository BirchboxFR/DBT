
SELECT 'IT' AS dw_country_code, 
t.* 
FROM `bdd_prod_it.da_eu_countries` t
WHERE {% if '__deleted' in columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}