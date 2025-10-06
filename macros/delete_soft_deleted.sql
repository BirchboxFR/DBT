{% macro delete_soft_deleted(countries, source_table, window_start) %}
  DELETE FROM {{ this }}
  WHERE STRUCT(dw_country_code, id) IN (
    {% for c in countries -%}
    SELECT AS STRUCT
      '{{ c.code }}' AS dw_country_code, 
      CAST(d.id AS INT64) AS id
    FROM `teamdata-291012.{{ c.dataset }}.{{ source_table }}` d
    WHERE d._airbyte_extracted_at >= {{ window_start }}
      AND SAFE.TIMESTAMP(NULLIF(d._ab_cdc_deleted_at,'')) IS NOT NULL
    {{ 'UNION ALL' if not loop.last }}
    {% endfor -%}
  )
{% endmacro %}