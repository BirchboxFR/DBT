SELECT dw_country_code,
       id AS current_box_id
FROM {{ ref('boxes') }}
WHERE shipping_status_id = 2
