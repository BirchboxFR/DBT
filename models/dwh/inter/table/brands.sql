{# ==========================
   PARAMÈTRES À CHANGER
   ========================== #}
{%- set source_table = "wp_jb_brands" -%}   
{%- set target_table = "inter.brands" -%}   

{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key=['dw_country_code','id'],
  cluster_by=["dw_country_code","id"]
) }}

{%- set countries = var('survey_countries') -%}
{%- set window_hours = 4 -%}
{%- set window_start -%}
TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ window_hours }} HOUR)
{%- endset -%}

{# ---------- POST HOOK : uniquement si la table existe déjà (vrai incrémental) ---------- #}
{% if is_incremental() %}
  {%- set to_delete_sql -%}
  DELETE FROM `teamdata-291012.{{ target_table }}`
  WHERE STRUCT(dw_country_code, id) IN (
    {%- for country in countries -%}
    SELECT AS STRUCT
      '{{ country.code }}' AS dw_country_code,
      CAST(d.id AS INT64) AS id
    FROM `teamdata-291012.{{ country.dataset }}.{{ source_table }}` d
    WHERE d._airbyte_extracted_at >= {{ window_start }}  -- prune SOURCE uniquement
      AND SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', NULLIF(d._ab_cdc_deleted_at,'')) IS NOT NULL
    {{ "UNION ALL" if not loop.last }}
    {%- endfor -%}
  );
  {%- endset -%}
  {{ config(post_hook=[ to_delete_sql ]) }}
{% endif %}

{# ---------- BUILD ---------- #}
{%- if is_incremental() -%}
  {# INCRÉMENTAL : actifs + fenêtre pour le pruning source #}
  {%- for country in countries %}
  SELECT
    '{{ country.code }}' AS dw_country_code,
    CAST(b.id AS INT64) AS id,
      b.url,
  b.name,
  b.post_id,
  b.visible,
  b.attr_moq,
  b.attr_mov,
  b.attr_tva,
  b.thumb_url,
  b.created_at,
  b.updated_at,
  b.description,
  b.attr_currency,
  b.attr_leadtime,
  b._ab_cdc_cursor,
  b.attr_po_format,
  b._ab_cdc_log_pos,
  b.attr_hide_recos,
  b.attr_po_sending,
  b._ab_cdc_log_file,
  b.attr_point_rouge,
  b.attr_franco_value,
  b.attr_rating_bonus,
  b.attr_specificites,
  b._ab_cdc_deleted_at,
  b._ab_cdc_updated_at,
  b.attr_group_post_id,
  b.attr_incoterms_box,
  b.attr_store_contact,
  b.attr_hide_sub_price,
  b.attr_incoterms_shop,
  b.attr_orders_contact,
  b.attr_payment_period,
  b.attr_no_sub_discount,
  b.attr_shipping_country,
  b.attr_marketing_contact,
  b.attr_is_active
  FROM `teamdata-291012.{{ country.dataset }}.{{ source_table }}` AS b
  WHERE NULLIF(b._ab_cdc_deleted_at, '') IS NULL
    AND b._airbyte_extracted_at >= {{ window_start }}
  {{ "UNION ALL" if not loop.last }}
  {%- endfor %}
{%- else -%}
  {# PREMIER RUN ou FULL REFRESH : pas de fenêtre, on charge tous les actifs #}
  {%- for country in countries %}
  SELECT
    '{{ country.code }}' AS dw_country_code,
    CAST(b.id AS INT64) AS id,
      b.url,
  b.name,
  b.post_id,
  b.visible,
  b.attr_moq,
  b.attr_mov,
  b.attr_tva,
  b.thumb_url,
  b.created_at,
  b.updated_at,
  b.description,
  b.attr_currency,
  b.attr_leadtime,
  b._ab_cdc_cursor,
  b.attr_po_format,
  b._ab_cdc_log_pos,
  b.attr_hide_recos,
  b.attr_po_sending,
  b._ab_cdc_log_file,
  b.attr_point_rouge,
  b.attr_franco_value,
  b.attr_rating_bonus,
  b.attr_specificites,
  b._ab_cdc_deleted_at,
  b._ab_cdc_updated_at,
  b.attr_group_post_id,
  b.attr_incoterms_box,
  b.attr_store_contact,
  b.attr_hide_sub_price,
  b.attr_incoterms_shop,
  b.attr_orders_contact,
  b.attr_payment_period,
  b.attr_no_sub_discount,
  b.attr_shipping_country,
  b.attr_marketing_contact,
  b.attr_is_active
  FROM `teamdata-291012.{{ country.dataset }}.{{ source_table }}` AS b
  WHERE NULLIF(b._ab_cdc_deleted_at, '') IS NULL
  {{ "UNION ALL" if not loop.last }}
  {%- endfor %}
{%- endif -%}
