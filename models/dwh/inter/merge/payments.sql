{{ config(
    partition_by={
      "field": "_airbyte_extracted_at", 
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["dw_country_code", "id"]
) }}

{%- set countries = var('survey_countries') -%}

--- partie pays

{%- set delete_hooks = [] -%}
{%- for country in countries -%}
  {%- set delete_sql -%}

  {%- endset -%}
  {%- do delete_hooks.append(delete_sql) -%}
{%- endfor -%}

{{ config(
    post_hook=delete_hooks
) }}
-- debug

{%- for country in countries %}
SELECT 
  '{{ country.code }}' as dw_country_code,
  id,
  user_id,
  order_id,
  sub_id,
  payment_gateway_id,
  transaction_id,
  amount,
  payment_profile_id,
  auto,
  status_id,
  payment_method_id,
  data,
  date(date) AS date,
  created_at,
  updated_at,
  _airbyte_extracted_at
FROM `teamdata-291012.{{ country.dataset }}.wp_jb_payments`
WHERE `_ab_cdc_deleted_at` IS NULL
{% if is_incremental() %}
  AND `_airbyte_extracted_at` >= timestamp_SUB(CURRENT_timestamp(), INTERVAL 2 HOUR)
{% endif %}
{{ "UNION ALL" if not loop.last }}
{%- endfor %}


{% if not is_incremental() %}
UNION ALL
-- Archives (seulement en full refresh)
SELECT * FROM {{ ref('archives_payments') }}
{% endif %}