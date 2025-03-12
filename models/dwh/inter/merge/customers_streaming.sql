WITH all_users AS (
    SELECT dw_country_code, email,id,
        MAX(user_id) AS user_id 
    FROM (   
        SELECT 
            dw_country_code, id,
            email, 
            NULL AS user_id
        FROM inter.optin
        
        UNION ALL
        
        SELECT 
            dw_country_code, null as id,
            user_email AS email, 
            id AS user_id
        FROM {{ ref('users') }}
        WHERE user_login <> 'DELETED'
    ) combined_users
    GROUP BY dw_country_code, email,id
)

SELECT 
    coalesce(cast(au.user_id as string),concat('prosp_',au.id) ) id,
    au.dw_country_code,
    au.user_id,
    case when au.user_id is null then true else false end as is_prospect,
    u.user_email LIKE '%@blissim%' OR u.user_email LIKE '%@birchbox%' AS is_admin,
    au.email,
    u.user_firstname AS firstname,
    u.user_lastname AS lastname,
FROM all_users au
left join {{ ref('users') }} u on u.id=au.user_id and u.dw_country_code=au.dw_country_code
left join {{ ref('user_consent_details') }} uc on uc.user_id=u.id and uc.dw_country_code=u.dw_country_code
where au.user_id=2327271