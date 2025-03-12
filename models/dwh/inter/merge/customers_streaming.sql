WITH all_users AS (
    SELECT dw_country_code, email,
        MAX(user_id) AS user_id 
    FROM (   
        SELECT 
            dw_country_code, id,
            email, 
            NULL AS user_id
        FROM inter.optin
        
        UNION ALL
        
        SELECT 
            dw_country_code, 0 as id,
            user_email AS email, 
            id AS user_id
        FROM {{ ref('users') }}
        WHERE user_login <> 'DELETED'
    ) combined_users
    GROUP BY dw_country_code, email
)

SELECT 
    coalesce(user_id,cast(concat('999999',id) as int64 )) id,
    au.dw_country_code,
    user_id,
    case when user_id is null then true else false end as is_prospect,
    au.email
FROM all_users au
left join {{ ref('users') }} u on u.id=au.user_id and u.dw_country_code=au.dw_country_code