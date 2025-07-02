
WITH source_data AS (
    -- Données France
    SELECT 
        id,
        created_at,
        updated_at,
        next_box_sub_id,
        current_box_sub_id,
        'FR' as dw_country_code,
        `_ab_cdc_updated_at` as source_updated_at
    FROM `teamdata-291012.bdd_prod_fr.wp_jb_yearly_check`
    WHERE `_ab_cdc_deleted_at` IS NULL
    {% if is_incremental() %}
        AND `_ab_cdc_updated_at` >= (
            SELECT COALESCE(MAX(source_updated_at), '1900-01-01') 
            FROM {{ this }}
        )
    {% endif %}

    UNION ALL

    -- Données Espagne
    SELECT 
        id,
        created_at,
        updated_at,
        next_box_sub_id,
        current_box_sub_id,
        'ES' as dw_country_code,
        `_ab_cdc_updated_at` as source_updated_at
    FROM `teamdata-291012.bdd_prod_es.wp_jb_yearly_check`
    WHERE `_ab_cdc_deleted_at` IS NULL
    {% if is_incremental() %}
        AND `_ab_cdc_updated_at` >= (
            SELECT COALESCE(MAX(source_updated_at), '1900-01-01') 
            FROM {{ this }}
        )
    {% endif %}
)

SELECT 
    id,
    created_at,
    updated_at,
    next_box_sub_id,
    current_box_sub_id,
    dw_country_code,
    source_updated_at,
    CURRENT_TIMESTAMP() as dbt_processed_at
FROM source_data