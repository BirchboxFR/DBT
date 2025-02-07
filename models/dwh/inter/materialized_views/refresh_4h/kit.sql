{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_sublissim', identifier='kit_raw')) -%}

SELECT 
t.*
FROM `bdd_prod_sublissim.kit_raw` t
WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}