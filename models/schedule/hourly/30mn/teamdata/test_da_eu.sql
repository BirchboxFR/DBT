{% set tables = ['bdd_prod_fr', 'bdd_prod_de', 'bdd_prod_es', 'bdd_prod_it'] %}
{% set countries = ['FR', 'DE', 'ES', 'IT'] %}

{% for table, country in zip(tables, countries) %}
 {% set check_deleted = adapter.get_columns_in_relation(ref(table)) %}
 SELECT 
   '{{country}}' AS dw_country_code,
   t.*,
   {% if '__deleted' in check_deleted | map(attribute='name') %}t.__deleted{% else %}false{% endif %} as is_deleted
 FROM `{{table}}.da_eu_countries` t
 {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}