{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_payment_profiles')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_payment_profiles')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_payment_profiles')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_payment_profiles')) -%}

SELECT 'FR' AS dw_country_code,
id,
  alias,
  atos_alias,
  recurring_reference,
  user_id,
  payment_gateway_id,
  SAFE_CAST(card_validity AS STRING) AS card_validity,
  card_number,
  last_fail_reason_id,
  flagged_for_update,
  created,
  updated,
  remember,
  card_holder_fullname,
  first_psp_reference
FROM `bdd_prod_fr.wp_jb_payment_profiles` t
WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}

UNION ALL

SELECT 'DE' AS dw_country_code,
id,
  alias,
  atos_alias,
  recurring_reference,
  user_id,
  payment_gateway_id,
  SAFE_CAST(card_validity AS STRING) AS card_validity,
  card_number,
  last_fail_reason_id,
  flagged_for_update,
  created,
  updated,
  remember,
  card_holder_fullname,
  first_psp_reference
FROM `bdd_prod_de.wp_jb_payment_profiles` t
WHERE {% if '__deleted' in de_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}

UNION ALL

SELECT 'ES' AS dw_country_code,
id,
  alias,
  atos_alias,
  recurring_reference,
  user_id,
  payment_gateway_id,
  SAFE_CAST(card_validity AS STRING) AS card_validity,
  card_number,
  last_fail_reason_id,
  flagged_for_update,
  created,
  updated,
  remember,
  card_holder_fullname,
  first_psp_reference
FROM `bdd_prod_es.wp_jb_payment_profiles` t
WHERE {% if '__deleted' in es_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}

UNION ALL

SELECT 'IT' AS dw_country_code,
id,
  alias,
  atos_alias,
  recurring_reference,
  user_id,
  payment_gateway_id,
  SAFE_CAST(card_validity AS STRING) AS card_validity,
  card_number,
  last_fail_reason_id,
  flagged_for_update,
  created,
  updated,
  remember,
  card_holder_fullname,
  first_psp_reference
FROM `bdd_prod_it.wp_jb_payment_profiles` t
WHERE {% if '__deleted' in it_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}