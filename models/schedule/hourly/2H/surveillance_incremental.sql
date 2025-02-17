{{
    config(
        materialized='incremental',
        partition_by={
            'field': 'creation_time',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
    )
}}

SELECT 
    user_email,
    destination_table.table_id,
    query,
    creation_time,
    sum(total_bytes_billed) as total_bytes_billed,
    (SUM(total_bytes_billed) / 1099511627776) * 6.5 as Go_billed
FROM region-europe-west1.INFORMATION_SCHEMA.JOBS
WHERE 
    {% if is_incremental() %}
        -- Dans un run incrémental, prendre les données depuis le dernier run
        creation_time > (SELECT max(creation_time) FROM {{ this }})
    {% else %}
        -- Dans un run complet, garder votre logique initiale de 4 mois
        date_diff(current_date(), date(creation_time), month) < 4
    {% endif %}
GROUP BY 1, 2, 3, 4