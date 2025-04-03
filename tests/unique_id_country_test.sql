
{% set models_to_test = [
    'users',
    'tags','boxes','choose_users'
] %}

{% for model_name in models_to_test %}
    {% if not loop.first %}
        union all
    {% endif %}
    
    select
        '{{ model_name }}' as model_name,
        id,
        dw_country_code,
        count(*) as records_count
    from {{ ref(model_name) }}
    where id is not null
      and dw_country_code is not null
    group by id, dw_country_code
    having count(*) > 1
{% endfor %}