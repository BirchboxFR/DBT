{% macro set_materialized_view_settings() %}
    {% set query %}
        alter materialized view {{ this }}
        set OPTIONS(
            enable_refresh=True,
            refresh_interval_minutes=120,
            max_staleness=INTERVAL '8:0:0' HOUR TO SECOND
        )
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}