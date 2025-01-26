{% macro set_materialized_view_settings() %}
ALTER MATERIALIZED VIEW `teamdata-291012`.`inter`.`christmas_offer`
SET OPTIONS(
   enable_refresh=True,
   refresh_interval_minutes=120,
   max_staleness=INTERVAL '8:0:0' HOUR TO SECOND
)
{% endmacro %}