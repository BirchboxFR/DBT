{{ 
  config(
    partition_by = {
      "field": "date",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by = ['dw_country_code'] 
  ) 
}}

{% set countries = ['fr', 'de', 'es', 'it'] %}
-------------
{% set lookback_hours = 2 %}
------- 
{% set column_map = {} %}
{%- for country in countries -%}
  {%- set rel = api.Relation.create(schema='bdd_prod_' ~ country, identifier='wp_jb_payments') -%}
  {%- set column_map = column_map.update({country: adapter.get_columns_in_relation(rel)}) or column_map -%}
{%- endfor %}

{% set queries = [] %}
{%- for country in countries -%}
  {% set columns = column_map[country] | map(attribute='name') | list %}
  {% set has_deleted = '__deleted' in columns %}
  {% set query %}
    SELECT '{{ country | upper }}' AS dw_country_code,
      id,user_id,order_id,sub_id,payment_gateway_id,transaction_id,amount,payment_profile_id,
      auto,status_id, payment_method_id,data,date(date) AS date,created_at,updated_at
    FROM `bdd_prod_{{ country }}.wp_jb_payments` t
    WHERE
      {% if has_deleted %}(t.__deleted IS NULL OR t.__deleted = false) AND{% endif %}
      {% if is_incremental() %}
        t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
      {% else %}
        TRUE
      {% endif %}
  {% endset %}
  {% do queries.append(query) %}
{%- endfor %}

{{ queries | join('\nUNION ALL\n') }}
