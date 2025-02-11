
WITH splio_data_dedup AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT ContactID AS email,
           Status AS status,
           Event_date AS event_date,
           ROW_NUMBER() OVER (PARTITION BY CampaignID, ContactID, Status ORDER BY Event_date) rn
    FROM crm.splio_events where event_date is not null
  )
  WHERE rn = 1
),

info_perso as (
SELECT distinct user_id ,dw_country_code,last_value(date) over ( partition by user_id order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ,
last_value(billing_country) over ( partition by user_id order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) billing_country,
last_value(billing_zipcode) over ( partition by user_id order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) billing_zipcode,
last_value(billing_phone) over ( partition by user_id order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) billing_phone,
last_value(billing_city) over ( partition by user_id order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) billing_city,
last_value(billing_adr1) over ( partition by user_id order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) billing_adress,
 FROM `teamdata-291012.inter.orders`

),
gender as (
SELECT dw_country_code,
o.user_id, 
CASE WHEN ARRAY_AGG(o.billing_civility ORDER BY o.date DESC LIMIT 1)[OFFSET(0)] = 'MISTER' THEN 'M' ELSE 'F' END AS gender
FROM 
inter.orders o
WHERE o.billing_civility IS NOT NULL
AND o.billing_civility <> ''
AND o.billing_civility <> 'NA'
GROUP BY ALL 
),
all_customers AS (
  SELECT dw_country_code, email, MAX(user_id) AS user_id
  FROM (
    SELECT dw_country_code, email, NULL AS user_id
    FROM inter.optin
    UNION ALL
    SELECT dw_country_code, user_email AS email, id AS user_id
    FROM inter.users
    WHERE user_login <> 'DELETED'
    UNION ALL
    SELECT 'FR' AS dw_country_code, email, NULL AS user_id
    FROM splio_data_dedup
    WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 36 MONTH)
    GROUP BY email
  )
  GROUP BY dw_country_code, email
),
user_data AS (
  SELECT u.dw_country_code,
         u.user_email AS email,
         uuid,
         CASE
          WHEN uml.list_box = 1 OR uml.list_sms = 1 OR uml.list_news = 1 OR uml.list_splendist = 1 OR uml.list_deals = 1 THEN TRUE
          ELSE FALSE
          END  AS optin,
          CASE
          WHEN uml.list_box = 1  OR uml.list_news = 1 OR uml.list_splendist = 1 OR uml.list_deals = 1 THEN TRUE
          ELSE FALSE
          END  AS optin_email,
         COALESCE(uml.list_box = 1, false) AS optin_box,
         COALESCE(uml.list_sms = 1, false) AS optin_sms,
         COALESCE(uml.list_news = 1, false) AS optin_news,
         COALESCE(uml.list_splendist = 1, false) AS optin_spl,
         COALESCE(uml.list_deals = 1, false) AS optin_deals,
         u.id AS user_id,
         u.user_email LIKE '%@blissim%' OR u.user_email LIKE '%@birchbox%' AS is_admin,
         u.user_firstname AS firstname,
         u.user_lastname AS lastname,
         CASE WHEN DATE(u.user_registered) >= '2011-01-01' THEN u.user_registered END AS registration_date, # If registration before 2011, consider problem in data
         CASE WHEN DATE_DIFF(CURRENT_DATE(), DATE(u.user_birthday), YEAR) <= 100 AND DATE_DIFF(CURRENT_DATE(), DATE(u.user_birthday), YEAR) >= 12 THEN u.user_birthday END AS birth_date,
         CASE WHEN DATE_DIFF(CURRENT_DATE(), DATE(u.user_birthday), YEAR) <= 100 AND DATE_DIFF(CURRENT_DATE(), DATE(u.user_birthday), YEAR) >= 12 THEN DATE_DIFF(CURRENT_DATE(), DATE(u.user_birthday), YEAR) END AS age
  FROM inter.users u
  LEFT JOIN inter.user_mailing_list uml ON u.dw_country_code = uml.dw_country_code AND u.id = uml.user_id
),
range_of_age_table AS (
  SELECT ud.dw_country_code,
         ud.user_id,
         concat(roa.id,' - ',roa.title) AS range_of_age
  FROM user_data ud
  INNER JOIN bdd_prod_fr.wp_jb_range_of_age roa ON ud.age >= roa.age_min AND ud.age <= roa.age_max
),
traffic_table AS (
  SELECT dw_country_code, user_id, MAX(last_login) AS last_login, MAX(ltm_has_seen_box_page) AS ltm_has_seen_box_page, MAX(ltm_has_seen_product_fullsize_page) AS ltm_has_seen_product_fullsize_page, MAX(ltm_has_seen_search_page) AS ltm_has_seen_search_page, MAX(ltm_has_seen_checkout_page) AS ltm_has_seen_checkout_page
  FROM (
    SELECT website_country_code AS dw_country_code, CAST(user_id AS INT64) AS user_id, MAX(session_start) AS last_login, MAX(session_with_box_page) AS ltm_has_seen_box_page, MAX(session_with_product_page) AS ltm_has_seen_product_fullsize_page, MAX(session_with_search_page) AS ltm_has_seen_search_page, MAX(session_with_checkout_step1) AS ltm_has_seen_checkout_page
    FROM `normalised-417010.traffic.sessions`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
    GROUP BY dw_country_code, user_id
    UNION ALL
    SELECT dw_country_code, id AS user_id, TIMESTAMP(last_login) AS last_login, False, False, False, False
    FROM inter.users
  )
  GROUP BY dw_country_code, user_id
),
crm_data AS (
  SELECT email,
         MAX(status = 'Open') AS open_email,
         MAX(status = 'Click') AS click,
         MAX(CASE WHEN status = 'Open' THEN event_date END) AS date_last_open_email,
         MAX(CASE WHEN status = 'Click' THEN event_date END) AS date_last_click_email,
         SAFE_DIVIDE(COUNTIF(status = 'Click' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)), COUNTIF(status = 'Done' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR))) AS ltm_client_email_rate,
         SAFE_DIVIDE(COUNTIF(status = 'Open' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)), COUNTIF(status = 'Done' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR))) AS ltm_open_email_rate,
         COUNTIF(status = 'Click' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) AS ltm_click_email,
         COUNTIF(status = 'Open' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) AS ltm_open_email,
         COUNTIF(status = 'Done' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) AS ltm_nb_email
  FROM splio_data_dedup
  GROUP BY email
),
beauty_profile_table AS (
   WITH ANSWER_CHECK AS (
       SELECT dw_country_code, user_id, MAX(body_issues) as body_issues,max(skin_issues) as skin_issues, MAX( hair_issues) as hair_issues
       , MAX (hair_dream ) as hair_dream,  MAX(hair_dryer_use) as hair_dryer_use, MAX (  fragrance_preference) as fragrance_preference, 
       MAX(shop_location) as shop_location , MAX (makeup_discovery) as makeup_discovery
       FROM 
       (SELECT sq.dw_country_code,
       sr.user_id,
              CASE WHEN sq.id = 46259 THEN MAX(sa.id) END as body_issues,
              CASE WHEN sq.id = 46258 THEN MAX(sa.id) END as skin_issues,
              CASE WHEN sq.id = 46850 THEN MAX(sa.id) END as hair_issues, 
              CASE WHEN sq.id = 46851 THEN MAX(sa.id) END as hair_dream,   
              CASE WHEN sq.id = 46852 THEN MAX(sa.id) END as hair_dryer_use ,
              CASE WHEN sq.id = 46855 THEN MAX(sa.id) END as fragrance_preference, 
              CASE WHEN sq.id = 46860 THEN MAX(sa.id) END as shop_location, 
              CASE WHEN sq.id = 79045 THEN MAX(sa.id) END as makeup_discovery

  FROM inter.survey_questions sq
  INNER JOIN inter.survey_answers sa ON COALESCE(sq.parent_id, sq.id) = sa.question_id AND sa.dw_country_code = 'FR'
  INNER JOIN inter.survey_results sr ON sq.dw_country_code = sr.dw_country_code AND sq.survey_id = sr.survey_id
  INNER JOIN inter.survey_result_answers sra ON sra.dw_country_code = sq.dw_country_code AND sra.question_id = sq.id AND sra.result_id = sr.id AND sra.answer_id = sa.id
  WHERE sq.survey_id = 2639
  GROUP BY sq.dw_country_code, sr.user_id, sq.id) t
  GROUP BY user_id , dw_country_code

  )
  
  
  
  SELECT sq.dw_country_code,
         sr.user_id,
         MAX(ifnull(CASE WHEN sq.id = 46256 THEN sa.value END,CASE WHEN sq.id = 15424 THEN sa.value END)) AS skin_complexion,
         MAX(ifnull(CASE WHEN sq.id = 46257 THEN sa.value END,CASE WHEN sq.id = 15425 THEN sa.value END)) AS skin_type,

        -- QUESTION ID =  > Les problemes de peau qui me concernent  SKIN ISSUES 
        
         MAX(CASE WHEN sa.id = 113178 THEN true ELSE 
              CASE WHEN ac.skin_issues is null THEN NULL ELSE false END END) AS skin_redness,
         MAX(CASE WHEN sa.id = 113179 THEN true ELSE 
              CASE WHEN ac.skin_issues is null THEN NULL ELSE false END END) AS skin_sensitiveness,
         MAX(CASE WHEN sa.id = 113180 THEN true ELSE 
              CASE WHEN ac.skin_issues is null THEN NULL ELSE false END END) AS skin_aging,
         MAX(CASE WHEN sa.id = 113181 THEN true ELSE 
              CASE WHEN ac.skin_issues is null THEN NULL ELSE false END END) AS skin_acne,
         MAX(CASE WHEN sa.id = 113182 THEN true ELSE 
              CASE WHEN ac.skin_issues is null THEN NULL ELSE false END END) AS skin_dilated_pores,
         MAX(CASE WHEN sa.id = 113183 THEN true ELSE
              CASE WHEN ac.skin_issues is null THEN NULL ELSE false END END) AS skin_dehydration,
         MAX(CASE WHEN sa.id = 113184 THEN true ELSE 
              CASE WHEN ac.skin_issues is null THEN NULL ELSE false END END) AS skin_eye_bags,
         MAX(CASE WHEN sa.id = 113185 THEN true ELSE 
              CASE WHEN ac.skin_issues is null THEN NULL ELSE false END END) AS skin_dullness,
         MAX(CASE WHEN sa.id = 113186 THEN true ELSE 
              CASE WHEN ac.skin_issues is null THEN NULL ELSE false END END) AS skin_no_problem,
         MAX(CASE WHEN sa.id = 153850 THEN true ELSE 
              CASE WHEN ac.skin_issues is null THEN NULL ELSE false END END) AS skin_spots,
         MAX(CASE WHEN sa.id = 153851 THEN true ELSE 
              CASE WHEN ac.skin_issues is null THEN NULL ELSE false END END) AS skin_wrinkles,

        -- QUESTION ID = > Au niveau de mon corps je me sens concernée par  BODY ISSUES 

        
         MAX(CASE WHEN sa.id = 113187 THEN true ELSE 
              CASE WHEN ac.body_issues is null THEN NULL ELSE false END END)  AS body_stretch_marks,
         MAX(CASE WHEN sa.id = 113188 THEN true ELSE 
              CASE WHEN ac.body_issues is null THEN NULL ELSE false END END)  AS body_cellulite,
         MAX(CASE WHEN sa.id = 113189 THEN true ELSE 
              CASE WHEN ac.body_issues is null THEN NULL ELSE false END END)  AS body_lack_firmness,
         MAX(CASE WHEN sa.id = 113190 THEN true ELSE 
              CASE WHEN ac.body_issues is null THEN NULL ELSE false END END) AS body_dry_skin,
         MAX(CASE WHEN sa.id = 113191 THEN true ELSE 
              CASE WHEN ac.body_issues is null THEN NULL ELSE false END END)  AS body_water_retention,
         MAX(CASE WHEN sa.id = 113192 THEN true ELSE 
              CASE WHEN ac.body_issues is null THEN NULL ELSE false END END) AS body_no_problem,
          MAX(CASE WHEN sa.id = 193350 THEN true ELSE 
              CASE WHEN ac.body_issues is null THEN NULL ELSE false END END) AS body_spots,

          MAX(ifnull(CASE WHEN sq.id = 46261 THEN sa.value END,CASE WHEN sq.id = 15422 THEN sa.value END)) AS hair_color,

         MAX(CASE WHEN sq.id = 46845 THEN sa.value END) AS hair_dye,
         MAX(CASE WHEN sq.id = 46846 THEN sa.value END) AS hair_thickness,
         MAX(CASE WHEN sq.id = 46847 THEN sa.value END) AS hair_type,
         MAX(CASE WHEN sq.id = 46848 THEN sa.value END) AS hair_scalp,
         MAX(CASE WHEN sq.id = 46849 THEN sa.value END) AS hair_style,

--- HAIR ISSUES : "Au quotidien mes cheveux sont" 
         MAX(CASE WHEN sa.id = 114597 THEN true ELSE 
              CASE WHEN ac.hair_issues is null THEN NULL ELSE false END END) AS hair_damaged,
         MAX(CASE WHEN sa.id = 114598 THEN true ELSE 
              CASE WHEN ac.hair_issues is null THEN NULL ELSE false END END)AS hair_split_end,
         MAX(CASE WHEN sa.id = 114599 THEN true ELSE 
              CASE WHEN ac.hair_issues is null THEN NULL ELSE false END END) AS hair_greasy,
         MAX(CASE WHEN sa.id = 114600 THEN true ELSE 
              CASE WHEN ac.hair_issues is null THEN NULL ELSE false END END)  AS hair_dried,
         MAX(CASE WHEN sa.id = 114601 THEN true ELSE 
              CASE WHEN ac.hair_issues is null THEN NULL ELSE false END END)  AS hair_dandruff,
         MAX(CASE WHEN sa.id = 114603 THEN true ELSE 
              CASE WHEN ac.hair_issues is null THEN NULL ELSE false END END)  AS hair_no_problem,
         MAX(CASE WHEN sa.id = 153858 THEN true ELSE
              CASE WHEN ac.hair_issues is null THEN NULL ELSE false END END)  AS hair_falls,

       -- HAIR DREAM : "Mon rêve serait d'avoir les cheveux "
         MAX(CASE WHEN sa.id = 114604 THEN true ELSE
              CASE WHEN ac.hair_dream is null THEN NULL ELSE false END END) AS want_hair_straight,
         MAX(CASE WHEN sa.id = 114605 THEN true ELSE 
              CASE WHEN ac.hair_dream is null THEN NULL ELSE false END END) AS want_hair_frizz_free,
         MAX(CASE WHEN sa.id = 114606 THEN true ELSE 
              CASE WHEN ac.hair_dream is null THEN NULL ELSE false END END) AS want_hair_volume,
         MAX(CASE WHEN sa.id = 114607 THEN true ELSE 
              CASE WHEN ac.hair_dream is null THEN NULL ELSE false END END) AS want_hair_shine,
         MAX(CASE WHEN sa.id = 114608 THEN true ELSE 
              CASE WHEN ac.hair_dream is null THEN NULL ELSE false END END) AS want_hair_soft,
         MAX(CASE WHEN sa.id = 114609 THEN true ELSE 
              CASE WHEN ac.hair_dream is null THEN NULL ELSE false END END) AS want_hair_less_thinning,
         MAX(CASE WHEN sa.id = 114610 THEN true ELSE 
              CASE WHEN ac.hair_dream is null THEN NULL ELSE false END END) AS want_hair_curly,
         MAX(CASE WHEN sa.id = 114611 THEN true ELSE 
              CASE WHEN ac.hair_dream is null THEN NULL ELSE false END END) AS want_hair_grow,

     
       -- HAIR DRYER USER : "De manière fréquente j'utilise "
         MAX(CASE WHEN sa.id = 114612 THEN true ELSE 
              CASE WHEN ac.hair_dryer_use is null THEN NULL ELSE false END END)AS use_hair_dryer,
         MAX(CASE WHEN sa.id = 114613 THEN true ELSE 
              CASE WHEN ac.hair_dryer_use is null THEN NULL ELSE false END END) AS use_hair_straightener,
         MAX(CASE WHEN sa.id = 114614 THEN true ELSE 
              CASE WHEN ac.hair_dryer_use is null THEN NULL ELSE false END END) AS use_hair_no_device,

         MAX(CASE WHEN sq.id = 46853 THEN sa.value END) AS beauty_routine,

       -- FRANGRANCE PREFERENCE : " Les parfums que j'apprécie le plus sont"
         MAX(CASE WHEN sa.id = 114623 THEN true ELSE 
              CASE WHEN ac.fragrance_preference is null THEN NULL ELSE false END END) AS fragrance_sweet,
         MAX(CASE WHEN sa.id = 114624 THEN true ELSE 
              CASE WHEN ac.fragrance_preference is null THEN NULL ELSE false END END)  AS fragrance_floral,
         MAX(CASE WHEN sa.id = 114625 THEN true ELSE 
              CASE WHEN ac.fragrance_preference is null THEN NULL ELSE false END END)  AS fragrance_spicy,
         MAX(CASE WHEN sa.id = 114626 THEN true ELSE 
              CASE WHEN ac.fragrance_preference is null THEN NULL ELSE false END END)  AS fragrance_fruity,
         MAX(CASE WHEN sa.id = 114627 THEN true ELSE 
              CASE WHEN ac.fragrance_preference is null THEN NULL ELSE false END END)  AS fragrance_woody,

       -- SHOP LOCATION AS "Le lieu où j'achète mes produits de beauté"
         MAX(CASE WHEN sa.id = 114657 THEN true ELSE 
              CASE WHEN ac.shop_location is null THEN NULL ELSE false END END) AS shop_perfumery,
         MAX(CASE WHEN sa.id = 114658 THEN true ELSE 
              CASE WHEN ac.shop_location is null THEN NULL ELSE false END END)AS shop_brand_store,
         MAX(CASE WHEN sa.id = 114659 THEN true ELSE 
              CASE WHEN ac.shop_location is null THEN NULL ELSE false END END) AS shop_hairdressing,
         MAX(CASE WHEN sa.id = 114660 THEN true ELSE 
              CASE WHEN ac.shop_location is null THEN NULL ELSE false END END)AS shop_pharmacy,
         MAX(CASE WHEN sa.id = 114661 THEN true ELSE 
              CASE WHEN ac.shop_location is null THEN NULL ELSE false END END) AS shop_hypermarket,
         MAX(CASE WHEN sa.id = 114662 THEN true ELSE 
              CASE WHEN ac.shop_location is null THEN NULL ELSE false END END)AS shop_bio_store,
         MAX(CASE WHEN sa.id = 114663 THEN true ELSE 
              CASE WHEN ac.shop_location is null THEN NULL ELSE false END END) AS shop_internet,


          -- En termes de maquillage, je serais curieuse de recevoir
          MAX(CASE WHEN sa.id = 193360 THEN true ELSE 
              CASE WHEN ac.makeup_discovery is null THEN NULL ELSE false END END) AS discovery_glitter,
          MAX(CASE WHEN sa.id = 193361 THEN true ELSE 
              CASE WHEN ac.makeup_discovery is null THEN NULL ELSE false END END)AS discovery_liners_mascaras,
          MAX(CASE WHEN sa.id =193362  THEN true ELSE 
              CASE WHEN ac.makeup_discovery is null THEN NULL ELSE false END END) AS discovery_colored_lipstick,
          MAX(CASE WHEN sa.id =193363  THEN true ELSE 
           CASE WHEN ac.makeup_discovery is null THEN NULL ELSE false END END) AS discovery_colored_nail_varnish,
             MAX(CASE WHEN sa.id =193364  THEN true ELSE 
           CASE WHEN ac.makeup_discovery is null THEN NULL ELSE false END END) AS discovery_colored_nude_makeup,
          MAX(CASE WHEN sa.id =193365  THEN true ELSE 
          CASE WHEN ac.makeup_discovery is null THEN NULL ELSE false END END) AS discovery_makeup,

         MAX(CASE WHEN sq.id = 46861 THEN sa.value END) AS beauty_budget,
         MAX(CASE WHEN sq.id = 62162 THEN sa.value END) AS skin_tone,
         MAX(CASE WHEN sq.id = 62163 THEN sa.value END) AS eyebrows,
         MAX(CASE WHEN sq.id = 62165 THEN sa.value END) AS face_care,
         MAX(CASE WHEN sq.id = 62166 THEN sa.value END) AS body_care,
         MAX(CASE WHEN sq.id = 62167 THEN sa.value END) AS bath_products,
         MAX(CASE WHEN sq.id = 62168 THEN sa.value END) AS makeup_general,
         MAX(CASE WHEN sq.id = 62169 THEN sa.value END) AS makeup_eyes,
         MAX(CASE WHEN sq.id = 62170 THEN sa.value END) AS makeup_lips,
         MAX(CASE WHEN sq.id = 62171 THEN sa.value END) AS makeup_eyebrows,
         MAX(CASE WHEN sq.id = 62172 THEN sa.value END) AS makeup_complexion,
         MAX(CASE WHEN sq.id = 62173 THEN sa.value END) AS makeup_nails,
         MAX(CASE WHEN sq.id = 62174 THEN sa.value END) AS hair_shampoo,
         MAX(CASE WHEN sq.id = 62175 THEN sa.value END) AS hair_conditioner,
         MAX(CASE WHEN sq.id = 62176 THEN sa.value END) AS hair_mask,
         MAX(CASE WHEN sq.id = 62177 THEN sa.value END) AS hair_styling,
         MAX(CASE WHEN sq.id = 62178 THEN sa.value END) AS accessories,
         MAX(CASE WHEN sq.id = 62179 THEN sa.value END) AS food_supplements,
         MAX(CASE WHEN sq.id = 62180 THEN sa.value END) AS green_natural_products,
         MAX(CASE WHEN sq.id = 62181 THEN sa.value END) AS slimming_products, 
         MAX(CASE WHEN sq.id = 66856 THEN sa.value END ) AS perfumes,
         MAX(CASE WHEN sq.id = 79039 THEN sa.value END) AS self_taining, 
         MAX(CASE WHEN sq.id = 79043 THEN sa.value END) AS solid_cosmetics,
         MAX(CASE WHEN sq.id = 79040 THEN sa.value END) AS hair_products
  FROM inter.survey_questions sq
  INNER JOIN inter.survey_answers sa ON COALESCE(sq.parent_id, sq.id) = sa.question_id AND sa.dw_country_code = 'FR'
  INNER JOIN inter.survey_results sr ON sq.dw_country_code = sr.dw_country_code AND sq.survey_id = sr.survey_id
  INNER JOIN inter.survey_result_answers sra ON sra.dw_country_code = sq.dw_country_code AND sra.question_id = sq.id AND sra.result_id = sr.id AND sra.answer_id = sa.id
  LEFT JOIN ANSWER_CHECK ac on ac.dw_country_code = sr.dw_country_code and ac.user_id = sr.user_id
  WHERE sq.survey_id = 2639
  GROUP BY sq.dw_country_code, sr.user_id
),
box_sales_one_line_user AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT dw_country_code,
           user_id,
           order_detail_id,
           sub_id,
           box_id,
           self,
           gift,
           coupon_engagement,
           year,
           month,
           date,
           coupon_code,
           coupon_code_id,
           payment_status,
           raffed,
           discount,
           net_revenue,
           gross_profit,
           ROW_NUMBER() OVER (PARTITION BY dw_country_code, user_id, box_id ORDER BY order_detail_id) rn
    FROM sales.box_sales
  )
  WHERE rn = 1
),
initial_box_table AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT dw_country_code,
           user_id,
           order_detail_id,
           CASE WHEN self = 1 THEN 'self' WHEN gift = 1 THEN 'gift' END AS initial_sub_type,
           coupon_engagement = 'engaged' AS initial_is_committed,
           year AS initial_box_year,
           month AS initial_box_month,
           date AS initial_box_date,
           coupon_code AS initial_coupon_code,
           CASE WHEN coupon_code_id > 0 THEN coupon_code_id END AS initial_coupon_code_id,
           ROW_NUMBER() OVER (PARTITION BY dw_country_code, user_id ORDER BY box_id) rn
    FROM box_sales_one_line_user bs
  )
  WHERE rn = 1
),
current_box_table AS (
  SELECT bs.dw_country_code,
         bs.user_id,
         bs.coupon_code AS current_coupon_code,
         CASE WHEN bs.coupon_code_id > 0 THEN bs.coupon_code_id END AS current_coupon_code_id,
         CASE WHEN bs.self = 1 THEN 'self' WHEN bs.gift = 1 THEN 'gift' END AS current_sub_type,
         ibt.order_detail_id IS NOT NULL AS current_is_initial,
         bs.coupon_engagement = 'engaged' AS current_is_committed,
         bs.payment_status = 'paid' AS current_box_paid,
         bs.payment_status = 'forthcoming' AS current_box_forthcoming,
         COALESCE(bs_next.payment_status = 'paid', False) AS next_box_paid,
         COALESCE(bs_next.payment_status = 'forthcoming', False) AS next_box_forthcoming,
         bs_next.box_id IS NOT NULL AS next_box_active,
         bs_next.box_id IS NULL AS next_box_churn,
         COALESCE(bs_prev.payment_status = 'paid', False) AS paid_prev_box,
         COALESCE(bs_prev_prev.payment_status = 'paid', False) AS paid_prev_prev_box
  FROM box_sales_one_line_user bs
  INNER JOIN snippets.current_box cb ON bs.dw_country_code = cb.dw_country_code AND bs.box_id = cb.current_box_id
  INNER JOIN inter.order_detail_sub s ON bs.dw_country_code = s.dw_country_code AND bs.sub_id = s.id
  LEFT JOIN initial_box_table ibt ON bs.dw_country_code = ibt.dw_country_code AND bs.order_detail_id = ibt.order_detail_id
  LEFT JOIN box_sales_one_line_user bs_prev ON bs.dw_country_code = bs_prev.dw_country_code AND bs.box_id = bs_prev.box_id + 1 AND bs.user_id = bs_prev.user_id
  LEFT JOIN box_sales_one_line_user bs_prev_prev ON bs.dw_country_code = bs_prev_prev.dw_country_code AND bs.box_id = bs_prev_prev.box_id + 2 AND bs.user_id = bs_prev_prev.user_id
  LEFT JOIN box_sales_one_line_user bs_next ON bs.dw_country_code = bs_next.dw_country_code AND bs.box_id = bs_next.box_id - 1 AND bs.user_id = bs_next.user_id
),
last_box_table AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT dw_country_code,
           user_id,
           order_detail_id,
           box_id,
           year AS last_box_paid_year,
           month AS last_box_paid_month,
           date AS last_box_paid_date,
           ROW_NUMBER() OVER (PARTITION BY dw_country_code, user_id ORDER BY box_id DESC) rn
    FROM box_sales_one_line_user
    WHERE payment_status = 'paid'
  )
  WHERE rn = 1
),
box_stats_table AS (
  SELECT bs.dw_country_code,
         bs.user_id,
         COUNTIF(bs.payment_status = 'paid') AS nb_box_paid,
         array_agg(struct(bs.box_id as box_id,payment_status as payment_status) order by bs.box_id desc)as array_boxes,
         COUNTIF(bs.payment_status = 'paid' AND lbt.order_detail_id IS NOT NULL) AS last_consecutive_box_paid,
         COUNTIF(bs.payment_status = 'paid' AND bs.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) AS ltm_nb_box_paid,
         COUNTIF(bs.payment_status = 'paid' AND lbt.order_detail_id IS NOT NULL AND bs.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) AS ltm_last_consecutive_box_paid,
         SUM(CASE WHEN bs.payment_status = 'paid' THEN net_revenue ELSE 0 END) AS box_net_revenue,
         SAFE_DIVIDE(SUM(CASE WHEN bs.payment_status = 'paid' THEN discount ELSE 0 END), COUNTIF(bs.payment_status = 'paid')) AS box_average_discount,
         SUM(CASE WHEN bs.payment_status = 'paid' THEN gross_profit ELSE 0 END) AS box_gross_profit,
         MAX(gift = 1) AS is_ever_gifted,
         MAX(self = 1) AS is_ever_self,
         MAX(raffed = 1) AS is_raffed
  FROM box_sales_one_line_user bs
  LEFT JOIN last_box_table lbt ON bs.dw_country_code = lbt.dw_country_code AND bs.order_detail_id = lbt.order_detail_id
  GROUP BY bs.dw_country_code,
           bs.user_id
),
sub_status_table AS (
  SELECT ac.dw_country_code,
         ac.email,
         ac.user_id,
         CASE WHEN MAX(bs_ever.user_id) IS NULL THEN 'NEVERSUB'
              WHEN MAX(bs.user_id IS NOT NULL) THEN 'SUB'
              ELSE 'CHURN'
         END AS box_sub_status
  FROM all_customers ac
  INNER JOIN snippets.current_box cb ON ac.dw_country_code = cb.dw_country_code
  LEFT JOIN box_sales_one_line_user bs_ever ON ac.dw_country_code = bs_ever.dw_country_code AND ac.user_id = bs_ever.user_id
  LEFT JOIN sales.box_sales bs ON ac.dw_country_code = bs.dw_country_code AND ac.user_id = bs.user_id AND bs.box_id = cb.current_box_id
  GROUP BY ac.dw_country_code,
           ac.email,
           ac.user_id
),
sub_status_table_before AS (
  SELECT ac.dw_country_code,
         ac.email,
         ac.user_id,
         CASE WHEN MAX(bs_ever.user_id) IS NULL THEN 'NEVERSUB'
              WHEN MAX(bs.user_id IS NOT NULL) THEN 'SUB'
              ELSE 'CHURN'
         END AS box_sub_status_before
  FROM all_customers ac
  INNER JOIN snippets.current_box cb ON ac.dw_country_code = cb.dw_country_code
  LEFT JOIN box_sales_one_line_user bs_ever ON ac.dw_country_code = bs_ever.dw_country_code AND ac.user_id = bs_ever.user_id
  LEFT JOIN sales.box_sales bs ON ac.dw_country_code = bs.dw_country_code AND ac.user_id = bs.user_id AND bs.box_id = cb.current_box_id-1
  GROUP BY ac.dw_country_code,
           ac.email,
           ac.user_id
),
raffer_table AS (
  SELECT DISTINCT dw_country_code,
         parent_user_id AS user_id,
         True AS is_raffer
  FROM inter.raf
),
choose_table AS (
  SELECT bs.dw_country_code,
         bs.user_id,
         MAX(cu.id IS NOT NULL) AS chose_ever,
         SAFE_DIVIDE(COUNTIF(bs.payment_status = 'paid' AND cu.id IS NOT NULL), COUNTIF(bs.payment_status = 'paid')) AS choose_participation_rate,
         SAFE_DIVIDE(COUNTIF(bs.payment_status = 'paid' AND cu.id IS NOT NULL AND bs.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)), COUNTIF(bs.payment_status = 'paid' AND bs.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR))) AS ltm_choose_participation_rate
  FROM box_sales_one_line_user bs
  INNER JOIN (SELECT DISTINCT dw_country_code, box_id FROM inter.choose_forms) cf ON bs.dw_country_code = cf.dw_country_code AND bs.box_id = cf.box_id
  LEFT JOIN inter.choose_users cu ON bs.dw_country_code = cu.dw_country_code AND bs.user_id = cu.user_id
  GROUP BY bs.dw_country_code,
           bs.user_id
),
box_survey_results AS (
  SELECT ss.dw_country_code,
         ss.box_id,
         sr.user_id
  FROM inter.survey_surveys ss
  INNER JOIN inter.survey_results sr ON ss.dw_country_code = sr.dw_country_code AND ss.id = sr.survey_id
  WHERE ss.open_date >= '2011-01-01'
  AND ss.type = 'BOX'
  AND sr.status = 'ANSWERED'
  AND ss.open_date <= CURRENT_DATE()
),
box_survey_answers AS (
  SELECT bs.dw_country_code,
         bsr.user_id,
         MAX(bsr.user_id IS NOT NULL) AS has_answered_box_survey,
         MAX(bsr.user_id IS NOT NULL AND bs.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) AS ltm_has_answered_box_survey,
         SAFE_DIVIDE(COUNTIF(bsr.user_id IS NOT NULL), COUNT(*)) AS rate_answering_box_survey,
         SAFE_DIVIDE(COUNTIF(bs.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) AND bsr.user_id IS NOT NULL), COUNTIF(bs.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR))) AS ltm_rate_answering_box_survey
  FROM box_sales_one_line_user bs
  LEFT JOIN box_survey_results bsr ON bs.dw_country_code = bsr.dw_country_code AND bs.user_id = bsr.user_id AND bs.box_id = bsr.box_id
  WHERE bs.payment_status = 'paid'
  GROUP BY bs.dw_country_code,
           bsr.user_id
),
shop_table AS (
  SELECT ac.dw_country_code,
         ac.user_id,
         MAX(ss.user_id IS NOT NULL) AS is_shopper,
         MAX(ss.product_codification_id = 0) AS is_shopper_fullsize,
         MAX(ss.product_codification_id IN (2, 8, 13)) AS is_shopper_exclusives,
         MAX(ss.product_codification_id = 2) AS is_shopper_lte,
         MAX(ss.product_codification_id = 8) AS is_shopper_splendist,
         MAX(ss.product_codification_id = 13) AS is_shopper_calendar,
         MIN(ss.order_date) AS first_shop_order_date,
         MIN(CASE WHEN ss.product_codification_id = 0 THEN ss.order_date END) AS first_fullsize_order_date,
         MIN(CASE WHEN ss.product_codification_id IN (2, 8, 13) THEN ss.order_date END) AS first_exclusive_order_date,
         MIN(CASE WHEN ss.product_codification_id =8 THEN ss.order_date END) AS first_splendist_order_date,
         MIN(CASE WHEN ss.product_codification_id =13 THEN ss.order_date END) AS first_calendar_order_date,
         MAX(ss.order_date) AS last_shop_order_date,
         MAX(CASE WHEN ss.product_codification_id = 0 THEN ss.order_date END) AS last_fullsize_order_date,
         MAX(CASE WHEN ss.product_codification_id IN (2, 8, 13) THEN ss.order_date END) AS last_exclusive_order_date,
         SUM(ss.net_revenue) AS shop_net_revenue,
         SUM(CASE WHEN ss.product_codification_id = 0 THEN ss.net_revenue END) AS fullsize_net_revenue
  FROM all_customers ac
  LEFT JOIN sales.shop_sales ss ON ac.dw_country_code = ss.dw_country_code AND ac.user_id = ss.user_id
  GROUP BY ac.dw_country_code,
           ac.user_id
),

first_order AS 
(
  SELECT c.dw_country_code,c.user_id,count(distinct o.id)as nb_shop_orders, min(o.id) AS first_order, min(o.date) as first_order_date,min(bs.order_id) as first_box_order,
    c.initial_box_date
  FROM user.customers c
  JOIN inter.orders o ON o.user_id = c.user_id AND o.dw_country_code = c.dw_country_code
    LEFT JOIN sales.box_sales bs ON o.ID = bs.order_id AND o.dw_country_code = bs.dw_country_code
  LEFT JOIN sales.shop_sales as ss ON ss.order_id = o.id AND ss.dw_country_code = o.dw_country_code
  WHERE o.status_id = 1
  GROUP BY c.dw_country_code, c.user_id,initial_box_date
),
first_order_type AS
(
  SELECT fo.dw_country_code, fo.user_id, fo.first_order, fo.first_order_date,nb_shop_orders, 
  MAX(CASE  WHEN bs.order_id IS NOT NULL THEN 'box' 
        WHEN ss.product_codification_id IN (2,8,13) THEN 'Exclusives'
      ELSE 'shop' END) AS first_order_type
  FROM first_order fo
  LEFT JOIN sales.box_sales bs ON fo.first_order = bs.order_id AND fo.dw_country_code = bs.dw_country_code
  LEFT JOIN sales.shop_sales as ss ON ss.order_id = fo.first_order AND ss.dw_country_code = fo.dw_country_code
  GROUP BY fo.dw_country_code, fo.user_id, fo.first_order,fo.first_order_date,nb_shop_orders
),
first_order_source AS
(
  SELECT c.dw_country_code, c.user_id,  MAX(t.source) AS source, MAX(t.campaign) AS campaign, MAX(t.support) AS support, MAX(t.device) AS device
  FROM first_order c
  JOIN `inter.ga_transactions` t ON t.order_id = c.first_order AND t.dw_country_code = c.dw_country_code
  WHERE t.dw_country_code IS NOT NULL
  GROUP BY c.dw_country_code, c.user_id
),
gp_box AS
(
  --GP box
 SELECT c.dw_country_code, c.user_id, 
  SUM( bs.gross_profit 
           
      ) AS gp_box_ever,
  SUM(CASE WHEN DATE_ADD(DATE(c.initial_box_date), INTERVAL 1 YEAR) > CURRENT_DATE THEN NULL
            WHEN bs.date <= DATE_ADD(DATE(c.initial_box_date), INTERVAL 1 YEAR) THEN bs.gross_profit 
            ELSE 0 END
      ) AS gp_box_year1,
    SUM(CASE WHEN  bs.date <= DATE_ADD(DATE(c.initial_box_date), INTERVAL 1 YEAR) THEN bs.gross_profit 
            ELSE 0 END
      ) AS gp_box_tmp_year1,  
  SUM(CASE  WHEN DATE_ADD(DATE(c.initial_box_date), INTERVAL 2 YEAR) > CURRENT_DATE THEN NULL
            WHEN bs.date <= DATE_ADD(DATE(c.initial_box_date), INTERVAL 2 YEAR) THEN bs.gross_profit 
            ELSE 0 END) AS gp_box_year2,
  SUM(CASE  WHEN DATE_ADD(DATE(c.initial_box_date), INTERVAL 3 YEAR) > CURRENT_DATE THEN NULL
            WHEN bs.date <= DATE_ADD(DATE(c.initial_box_date), INTERVAL 3 YEAR) THEN bs.gross_profit 
            ELSE 0 END) AS gp_box_year3,
  SUM(CASE  WHEN DATE_ADD(DATE(c.initial_box_date), INTERVAL 4 YEAR) > CURRENT_DATE THEN NULL
            WHEN bs.date <= DATE_ADD(DATE(c.initial_box_date), INTERVAL 4 YEAR) THEN bs.gross_profit 
            ELSE 0 END) AS gp_box_year4
  FROM first_order c
  JOIN inter.orders o ON o.id = c.first_box_order AND o.dw_country_code = c.dw_country_code
  JOIN sales.box_sales as bs ON bs.user_id = c.user_id AND  bs.dw_country_code = c.dw_country_code
  GROUP BY c.dw_country_code, c.user_id
),
gp_shop AS
(
  --GP shop & excl
  SELECT c.dw_country_code, c.user_id,
    SUM( ss.gross_profit
      ) AS gp_shop_ever, 
  SUM(CASE WHEN DATE_ADD(DATE(o.date), INTERVAL 1 YEAR) > CURRENT_DATE THEN NULL
            WHEN ss.order_date <= DATE_ADD(DATE(o.date), INTERVAL 1 YEAR) THEN ss.gross_profit 
            ELSE 0 END
      ) AS gp_shop_year1,
    SUM(CASE WHEN DATE_ADD(DATE(o.date), INTERVAL 1 YEAR) > CURRENT_DATE THEN NULL
            WHEN ss.order_date <= DATE_ADD(DATE(o.date), INTERVAL 1 YEAR) THEN ss.gross_profit 
            ELSE 0 END
      ) AS gp_shop_tmp_year1,
  SUM(CASE  WHEN DATE_ADD(DATE(o.date), INTERVAL 2 YEAR) > CURRENT_DATE THEN NULL
            WHEN ss.order_date <= DATE_ADD(DATE(o.date), INTERVAL 2 YEAR) THEN ss.gross_profit 
            ELSE 0 END) AS gp_shop_year2,
  SUM(CASE  WHEN DATE_ADD(DATE(o.date), INTERVAL 3 YEAR) > CURRENT_DATE THEN NULL
            WHEN ss.order_date <= DATE_ADD(DATE(o.date), INTERVAL 3 YEAR) THEN ss.gross_profit 
            ELSE 0 END) AS gp_shop_year3,
  SUM(CASE  WHEN DATE_ADD(DATE(o.date), INTERVAL 4 YEAR) > CURRENT_DATE THEN NULL
            WHEN ss.order_date <= DATE_ADD(DATE(o.date), INTERVAL 4 YEAR) THEN ss.gross_profit 
            ELSE 0 END) AS gp_shop_year4
  FROM first_order c
  JOIN inter.orders o ON o.id = c.first_order AND o.dw_country_code = c.dw_country_code
  JOIN `sales.shop_orders_margin` as ss ON ss.user_id = c.user_id AND  ss.dw_country_code = c.dw_country_code
  GROUP BY c.dw_country_code, c.user_id
)

SELECT ac.dw_country_code,
       ac.email,
       ac.user_id,
       uuid,
       ud.optin,
       ud.optin_email,
       ud.optin_box,
       ud.optin_news,
       ud.optin_spl,
       ud.optin_deals,
       ud.optin_sms,
       ud.is_admin,
       ud.firstname,
       ud.lastname,
       gender.gender,
       ud.registration_date,
       ud.birth_date,
       ud.age,
       ip.billing_phone,
       ip.billing_country,
       ip.billing_zipcode,
       ip.billing_city,
       ip.billing_adress,
       roa.range_of_age,
       tt.last_login,
       tt.ltm_has_seen_box_page,
       tt.ltm_has_seen_product_fullsize_page,
       tt.ltm_has_seen_search_page,
       tt.ltm_has_seen_checkout_page,
       cd.open_email,
       cd.click,
       cd.date_last_open_email,
       cd.date_last_click_email,
       cd.ltm_client_email_rate,
       cd.ltm_open_email_rate,
       cd.ltm_click_email,
       cd.ltm_open_email,
       cd.ltm_nb_email,
       bpt.skin_complexion,
       bpt.skin_type,
       bpt.skin_redness,
       bpt.skin_sensitiveness,
       bpt.skin_aging,
       bpt.skin_acne,
       bpt.skin_dilated_pores,
       bpt.skin_dehydration,
       bpt.skin_eye_bags,
       bpt.skin_dullness,
       bpt.skin_no_problem,
       bpt.skin_spots,
       bpt.skin_wrinkles,
       bpt.body_stretch_marks,
       bpt.body_cellulite,
       bpt.body_lack_firmness,
       bpt.body_dry_skin,
       bpt.body_water_retention,
       bpt.body_no_problem,
       bpt.body_spots,
       bpt.hair_color,
       bpt.hair_dye,
       bpt.hair_thickness,
       bpt.hair_type,
       bpt.hair_scalp,
       bpt.hair_style,
       bpt.hair_damaged,
       bpt.hair_split_end,
       bpt.hair_greasy,
       bpt.hair_dried,
       bpt.hair_dandruff,
       bpt.hair_no_problem,
       bpt.hair_falls,
       bpt.want_hair_straight,
       bpt.want_hair_frizz_free,
       bpt.want_hair_volume,
       bpt.want_hair_shine,
       bpt.want_hair_soft,
       bpt.want_hair_less_thinning,
       bpt.want_hair_curly,
       bpt.want_hair_grow,
       bpt.use_hair_dryer,
       bpt.use_hair_straightener,
       bpt.use_hair_no_device,
       bpt.beauty_routine,
       bpt.fragrance_sweet,
       bpt.fragrance_floral,
       bpt.fragrance_spicy,
       bpt.fragrance_fruity,
       bpt.fragrance_woody,
       bpt.shop_perfumery,
       bpt.shop_brand_store,
       bpt.shop_hairdressing,
       bpt.shop_pharmacy,
       bpt.shop_hypermarket,
       bpt.shop_bio_store,
       bpt.shop_internet,
       bpt.beauty_budget,
       bpt.skin_tone,
       bpt.eyebrows,
       bpt.face_care,
       bpt.body_care,
       bpt.bath_products,
       bpt.makeup_general,
       bpt.makeup_eyes,
       bpt.makeup_lips,
       bpt.makeup_eyebrows,
       bpt.makeup_complexion,
       bpt.makeup_nails,
       bpt.hair_shampoo,
       bpt.hair_conditioner,
       bpt.hair_mask,
       bpt.hair_styling,
       bpt.accessories,
       bpt.food_supplements,
       bpt.green_natural_products,
       bpt.slimming_products,
       bpt.perfumes,
      bpt.self_taining, 
          bpt.solid_cosmetics,
          bpt.hair_products, 
          bpt.discovery_glitter,
          bpt.discovery_liners_mascaras,
          bpt.discovery_colored_lipstick,
          bpt.discovery_colored_nail_varnish,
          bpt.discovery_colored_nude_makeup,
          bpt.discovery_makeup,
       sst.box_sub_status,
       sstb.box_sub_status_before,
       ibt.initial_sub_type,
       ibt.initial_is_committed,
       ibt.initial_box_year,
       ibt.initial_box_month,
       ibt.initial_box_date,
       ibt.initial_coupon_code,
       ibt.initial_coupon_code_id,
       cbt.current_coupon_code,
       cbt.current_coupon_code_id,
       cbt.current_sub_type,
       cbt.current_is_initial,
       cbt.current_is_committed,
       cbt.current_box_paid,
       cbt.current_box_forthcoming,
       cbt.next_box_paid,
       cbt.next_box_forthcoming,
       cbt.next_box_active,
       cbt.next_box_churn,
       cbt.paid_prev_box,
       cbt.paid_prev_prev_box,
       lbt.last_box_paid_year,
       lbt.last_box_paid_month,
       lbt.last_box_paid_date,
       bst.array_boxes,
       bst.nb_box_paid,
       bst.last_consecutive_box_paid,
       bst.ltm_nb_box_paid,
       bst.ltm_last_consecutive_box_paid,
       bst.box_net_revenue,
       bst.box_average_discount,
       bst.box_gross_profit,
       bst.is_ever_gifted,
       bst.is_ever_self,
       bst.is_raffed,
       rt.is_raffer,
       ct.chose_ever,
       ct.choose_participation_rate,
       ct.ltm_choose_participation_rate,
       bsa.has_answered_box_survey,
       bsa.ltm_has_answered_box_survey,
       bsa.rate_answering_box_survey,
       bsa.ltm_rate_answering_box_survey,
       st.is_shopper,
       st.is_shopper_fullsize,
       st.is_shopper_exclusives,
       st.is_shopper_lte,
       st.is_shopper_splendist,
       st.is_shopper_calendar,
       st.first_shop_order_date,
       st.first_fullsize_order_date,
       st.first_exclusive_order_date,
       first_splendist_order_date,
       first_calendar_order_date,
       st.last_shop_order_date,
       st.last_fullsize_order_date,
       st.last_exclusive_order_date,
       fot.first_order,
       fot.first_order_type,
       fot.first_order_date,
       fos.source AS first_order_source,
       fos.campaign AS first_order_campaign,
       fos.support AS first_order_support,
       fos.device AS first_order_device,
       nb_shop_orders,
       case when nb_shop_orders = 1 then '1'
       when nb_shop_orders between 2 and 5 then '2-5'
       when nb_shop_orders between 6 and 10 then '6-10'
       when nb_shop_orders >10 then '10+' end as group_shop_orders,
       ifnull(gp_box.gp_box_year1,0) AS ltv_box_year1,
       ifnull(gp_box.gp_box_tmp_year1,0) AS ltv_box_tmp_year1,
       ifnull(gp_box.gp_box_year2,0) AS ltv_box_year2,
       ifnull(gp_box.gp_box_year3,0) AS ltv_box_year3,
       ifnull(gp_box.gp_box_year4,0) AS ltv_box_year4,
       ifnull(gp_box.gp_box_ever,0) AS ltv_Box_ever,
       ifnull(gp_shop.gp_shop_ever,0) AS ltv_shop_ever,
       ifnull(gp_shop.gp_shop_year1,0) AS ltv_shop_year1, -- ltv fin de la première année complétée
       ifnull(gp_shop.gp_shop_tmp_year1,0) AS ltv_shop_tmp_year1, -- ltv sur l'année en cours 
       ifnull(gp_shop.gp_shop_year2,0) AS ltv_shop_year2,
       ifnull(gp_shop.gp_shop_year3,0) AS ltv_shop_year3,
       ifnull(gp_shop.gp_shop_year4,0) AS ltv_shop_year4,
       ifnull(gp_box.gp_box_ever,0) + ifnull(gp_shop.gp_shop_ever,0) AS ltv_ever,
       ifnull(gp_box.gp_box_year1,0) + ifnull(gp_shop.gp_shop_year1,0) AS ltv_year1,
       ifnull(gp_box.gp_box_tmp_year1,0) + ifnull(gp_shop.gp_shop_tmp_year1,0) AS ltv_tmp_year1,
        ifnull(gp_box.gp_box_year2,0) + ifnull(gp_shop.gp_shop_year2,0) AS ltv_year2,
       ifnull(gp_box.gp_box_year3,0) + ifnull(gp_shop.gp_shop_year3,0) AS ltv_year3,
       ifnull(gp_box.gp_box_year4,0) + ifnull(gp_shop.gp_shop_year4,0) AS ltv_year4,
       ltv.predicted_ltv AS predicted_ltv_year1
FROM all_customers ac
LEFT JOIN user_data ud ON ac.dw_country_code = ud.dw_country_code AND ac.user_id = ud.user_id
LEFT JOIN range_of_age_table roa ON ac.dw_country_code = roa.dw_country_code AND ac.user_id = roa.user_id
LEFT JOIN traffic_table tt ON ac.dw_country_code = tt.dw_country_code AND ac.user_id = tt.user_id
LEFT JOIN crm_data cd ON ac.dw_country_code = 'FR' AND ac.email = cd.email
LEFT JOIN beauty_profile_table bpt ON ac.dw_country_code = bpt.dw_country_code AND ac.user_id = bpt.user_id
LEFT JOIN sub_status_table sst ON ac.dw_country_code = sst.dw_country_code AND ac.email = sst.email
LEFT JOIN sub_status_table_before sstb ON ac.dw_country_code = sstb.dw_country_code AND ac.email = sstb.email
LEFT JOIN initial_box_table ibt ON ac.dw_country_code = ibt.dw_country_code AND ac.user_id = ibt.user_id
LEFT JOIN current_box_table cbt ON ac.dw_country_code = cbt.dw_country_code AND ac.user_id = cbt.user_id
LEFT JOIN last_box_table lbt ON ac.dw_country_code = lbt.dw_country_code AND ac.user_id = lbt.user_id
LEFT JOIN box_stats_table bst ON ac.dw_country_code = bst.dw_country_code AND ac.user_id = bst.user_id
LEFT JOIN raffer_table rt ON ac.dw_country_code = rt.dw_country_code AND ac.user_id = rt.user_id
left join info_perso ip on ip.user_id= ac.user_id and ip.dw_country_code=ac.dw_country_code
LEFT JOIN gender ON gender.user_id = ac.user_id AND gender.dw_country_code = ac.dw_country_code
LEFT JOIN choose_table ct ON ac.dw_country_code = ct.dw_country_code AND ac.user_id = ct.user_id
LEFT JOIN box_survey_answers bsa ON ac.dw_country_code = bsa.dw_country_code AND ac.user_id = bsa.user_id
LEFT JOIN shop_table st ON ac.dw_country_code = st.dw_country_code AND ac.user_id = st.user_id
LEFT JOIN first_order_type fot ON fot.dw_country_code = ac.dw_country_code AND fot.user_id = ac.user_id
LEFT JOIN first_order_source fos ON fos.dw_country_code = ac.dw_country_code AND fos.user_id = ac.user_id
LEFT JOIN gp_box ON gp_box.dw_country_code = ac.dw_country_code AND gp_box.user_id = ac.user_id
LEFT JOIN gp_shop ON gp_shop.dw_country_code = ac.dw_country_code AND gp_shop.user_id = ac.user_id
LEFT JOIN `teamdata-291012.predictive_ltv.ltv` ltv ON ltv.user_id = ac.user_id AND ac.dw_country_code = 'FR';