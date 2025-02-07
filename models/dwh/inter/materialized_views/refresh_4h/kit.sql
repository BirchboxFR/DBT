SELECT 
t.*
FROM `bdd_prod_sublissim.kit_raw` t
WHERE {% if '__deleted' in de_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}