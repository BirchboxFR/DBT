{% macro delete_soft_deleted(countries, source_table, window_start) %}
  DELETE FROM {{ this }} AS T
  WHERE EXISTS (
    SELECT 1
    FROM (
      {% for c in countries -%}
      SELECT '{{ c.code }}' AS dw_country_code, CAST(d.id AS INT64) AS id
      FROM `teamdata-291012.{{ c.dataset }}.{{ source_table }}` d
      WHERE d._airbyte_extracted_at >= {{ window_start }}
        AND SAFE.TIMESTAMP(NULLIF(d._ab_cdc_deleted_at,'')) IS NOT NULL
      {{ 'UNION ALL' if not loop.last }}
      {% endfor -%}
    ) S
    WHERE S.dw_country_code = T.dw_country_code
      AND S.id = T.id
  )
{% endmacro %}