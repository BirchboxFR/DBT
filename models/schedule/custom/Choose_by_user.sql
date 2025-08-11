   SELECT cu.user_id,
   concat(cu.dw_country_code,'_',cu.user_id)as user_key,
    box_id, cc.choice_name, cu.dw_country_code, cu.status_id, cu.created_at as choice_date, cf.name as form_name,cf.id as form_id,
    cc.id as choice_id
    FROM {{ ref('choose_users') }} cu
    JOIN {{ ref('choose_forms') }} cf ON cf.id = cu.form_id AND cf.dw_country_code = cu.dw_country_code
    JOIN {{ ref('choose_choices') }} cc ON cc.id = cu.choice_id AND cc.dw_country_code = cu.dw_country_code
    JOIN {{ ref('customers') }} cus ON cus.user_id = cu.user_id AND cus.dw_country_code = cu.dw_country_code
    group by all