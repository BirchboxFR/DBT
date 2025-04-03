-- tests/unique_id_country_multi_folders.sql

-- Liste des modèles de différents dossiers
{% set models_to_test = [
    -- Modèles du dossier 'merge'
    'users',
    'tags'
    
    -- Ajoutez tous les autres modèles que vous souhaitez tester
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