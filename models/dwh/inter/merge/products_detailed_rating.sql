WITH source_data AS (
    -- Données France
    SELECT 
        id,
        rating,
        created_at,
        field_name,
        field_slug,
        product_id,
        updated_at,
        'FR' as country_code,
        `_ab_cdc_updated_at` as source_updated_at
    FROM `teamdata-291012.bdd_prod_fr.wp_jb_products_detailed_rating`
    WHERE `_ab_cdc_deleted_at` IS NULL
    {% if is_incremental() %}
        AND `_ab_cdc_updated_at` > (SELECT MAX(source_updated_at) FROM {{ this }})
    {% endif %}

    UNION ALL

    -- Données Allemagne  
    SELECT 
        id,
        rating,
        created_at,
        field_name,
        field_slug,
        product_id,
        updated_at,
        'DE' as country_code,
        `_ab_cdc_updated_at` as source_updated_at
    FROM `teamdata-291012.bdd_prod_de.wp_jb_products_detailed_rating`
    WHERE `_ab_cdc_deleted_at` IS NULL
    {% if is_incremental() %}
        AND `_ab_cdc_updated_at` > (SELECT MAX(source_updated_at) FROM {{ this }})
    {% endif %}
)

SELECT 
    id,
    rating,
    created_at,
    field_name,
    field_slug,
    product_id,
    updated_at,
    dw_country_code,
    source_updated_at,
    CURRENT_TIMESTAMP() as dbt_processed_at
FROM source_data