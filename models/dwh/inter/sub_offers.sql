{{ config(
    materialized='table',
    partition_by={
      "field": "id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 1000000000,
        "interval": 100000
      }
    },
    cluster_by=['dw_country_code', 'code','parent_offer_id','validity_date']
) }}


{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_sub_offers')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_sub_offers')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_sub_offers')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_sub_offers')) -%}

SELECT 'FR' AS dw_country_code,
id,
  parent_offer_id,
  code,
  title,
  description,
  conditions,
  offer_type,
  offer_value,
  secondary_offer_value,
  SAFE_CAST(valid_from AS STRING) AS valid_from,
  SAFE_CAST(validity_date AS STRING) AS validity_date,
  max_use,
  count,
  sub_engagement_period,
  start_box,
  offer_target,
  subs_paid_in_advance,
  trigger,
  created_at,
  updated_at,
  created_by
  FROM `bdd_prod_fr.wp_jb_sub_offers` t
WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}

UNION ALL

SELECT 'DE' AS dw_country_code,
id,
  parent_offer_id,
  code,
  title,
  description,
  conditions,
  offer_type,
  offer_value,
  secondary_offer_value,
  SAFE_CAST(valid_from AS STRING) AS valid_from,
  SAFE_CAST(validity_date AS STRING) AS validity_date,
  max_use,
  count,
  sub_engagement_period,
  start_box,
  offer_target,
  subs_paid_in_advance,
  trigger,
  created_at,
  updated_at,
  created_by
FROM `bdd_prod_de.wp_jb_sub_offers` t
WHERE {% if '__deleted' in de_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}

UNION ALL

SELECT 'ES' AS dw_country_code,
id,
  parent_offer_id,
  code,
  title,
  description,
  conditions,
  offer_type,
  offer_value,
  secondary_offer_value,
  SAFE_CAST(valid_from AS STRING) AS valid_from,
  SAFE_CAST(validity_date AS STRING) AS validity_date,
  max_use,
  count,
  sub_engagement_period,
  start_box,
  offer_target,
  subs_paid_in_advance,
  trigger,
  created_at,
  updated_at,
  created_by
  FROM `bdd_prod_es.wp_jb_sub_offers` t
WHERE {% if '__deleted' in es_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}

UNION ALL

SELECT 'IT' AS dw_country_code,
id,
  parent_offer_id,
  code,
  title,
  description,
  conditions,
  offer_type,
  offer_value,
  secondary_offer_value,
  SAFE_CAST(valid_from AS STRING) AS valid_from,
  SAFE_CAST(validity_date AS STRING) AS validity_date,
  max_use,
  count,
  sub_engagement_period,
  start_box,
  offer_target,
  subs_paid_in_advance,
  trigger,
  created_at,
  updated_at,
  created_by
  FROM `bdd_prod_it.wp_jb_sub_offers` t
WHERE {% if '__deleted' in it_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}