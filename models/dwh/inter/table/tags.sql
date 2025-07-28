-- Configuration pour table complète (pas incrémental)
{{ config(
    partition_by={
      "field": "link_id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 100000000,
        "interval": 100000
      }
    },
    cluster_by=['dw_country_code']
) }}

{% set countries = [
  {'code': 'FR', 'schema': 'bdd_prod_fr'},
  {'code': 'DE', 'schema': 'bdd_prod_de'},
  {'code': 'ES', 'schema': 'bdd_prod_es'},
  {'code': 'IT', 'schema': 'bdd_prod_it'}
] %}

-- Union de tous les pays
{% for country in countries %}
SELECT 
  '{{ country.code }}' AS dw_country_code,
  t.id,
  t.link_id,
  t.value,
  t.type,
  t.timestamp,
  t.user_id,
  t._airbyte_extracted_at
FROM `{{ country.schema }}.wp_jb_tags` t
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}