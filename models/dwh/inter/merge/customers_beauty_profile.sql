


{{ config(
    partition_by={
"field": "last_update",
     "data_type": "date"
    },
    cluster_by=['dw_country_code', 'skin_redness','bath_products','hair_products']
) }}

--partition 
{% set lookback_hours = 2 %}
--lookback 2h

SELECT    concat(sq.dw_country_code,'_',cast(sr.user_id as string)) as ID,
          sq.dw_country_code,
       sr.user_id,
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
         MAX(CASE WHEN sq.id = 79040 THEN sa.value END) AS hair_products,

         MAX(CASE WHEN sq.id = 79045 and sa.id = 193360 THEN true 
              WHEN  sq.id = 79045  and sa.id is not  null THEN false ELSE null END) AS discovery_glitter,
          MAX(CASE WHEN sq.id = 79045 and sa.id = 193361 THEN true 
               WHEN  sq.id = 79045  and sa.id is not  null THEN false ELSE null END)AS discovery_liners_mascaras,
          MAX(CASE WHEN sq.id = 79045 and sa.id =193362 THEN true  
               WHEN  sq.id = 79045  and sa.id is not  null THEN false ELSE null END) AS discovery_colored_lipstick,
          MAX(CASE WHEN sq.id = 79045 and sa.id =193363  THEN true  
            WHEN  sq.id = 79045  and sa.id is not  null THEN false ELSE null END) AS discovery_colored_nail_varnish,
             MAX(CASE WHEN sq.id = 79045 and sa.id =193364 THEN true 
            WHEN  sq.id = 79045  and sa.id is not  null THEN false ELSE null END) AS discovery_colored_nude_makeup,
          MAX(CASE WHEN sq.id = 79045 and  sa.id =193365 THEN true 
           WHEN  sq.id = 79045  and sa.id is not  null THEN false ELSE null END) AS discovery_makeup,


                 -- SHOP LOCATION AS "Le lieu où j'achète mes produits de beauté"
         MAX(CASE WHEN sq.id = 46860 and sa.id = 114657 THEN true 
              WHEN  sq.id = 46860  and sa.id is not  null THEN false ELSE null END) AS shop_perfumery,
         MAX(CASE WHEN sq.id = 46860 and sa.id = 114658 THEN true 
              WHEN  sq.id = 46860  and sa.id is not  null THEN false ELSE null END)AS shop_brand_store,
         MAX(CASE WHEN sq.id = 46860 and sa.id = 114659 THEN true 
              WHEN  sq.id = 46860  and sa.id is not  null THEN false ELSE null END) AS shop_hairdressing,
         MAX(CASE WHEN sq.id = 46860 and sa.id = 114660 THEN true 
              WHEN  sq.id = 46860  and sa.id is not  null THEN false ELSE null END)AS shop_pharmacy,
         MAX(CASE WHEN sq.id = 46860 and sa.id = 114661 THEN true 
              WHEN  sq.id = 46860  and sa.id is not  null THEN false ELSE null END) AS shop_hypermarket,
         MAX(CASE WHEN sq.id = 46860 and sa.id = 114662 THEN true 
              WHEN  sq.id = 46860  and sa.id is not  null THEN false ELSE null END)AS shop_bio_store,
         MAX(CASE WHEN sq.id = 46860 and sa.id = 114663 THEN true 
              WHEN  sq.id = 46860  and sa.id is not  null THEN false ELSE null END) AS shop_internet,

                 -- FRAGRANCE PREFERENCE : " Les parfums que j'apprécie le plus sont"
                 MAX(CASE WHEN sq.id = 46855 and sa.id = 114623 THEN true 
         WHEN  sq.id = 46855  and sa.id is not  null THEN false ELSE null END) AS fragrance_sweet,
         MAX(CASE WHEN sq.id = 46855 and sa.id = 114624 THEN true 
              WHEN  sq.id = 46855  and sa.id is not  null THEN false ELSE null END)AS fragrance_floral,
         MAX(CASE WHEN sq.id = 46855 and sa.id = 114625 THEN true 
              WHEN  sq.id = 46855  and sa.id is not  null THEN false ELSE null END) AS fragrance_spicy,
         MAX(CASE WHEN sq.id = 46855 and sa.id = 114626 THEN true 
              WHEN  sq.id = 46855  and sa.id is not  null THEN false ELSE null END)AS fragrance_fruity,
         MAX(CASE WHEN sq.id = 46855 and sa.id = 114627 THEN true 
              WHEN  sq.id = 46855  and sa.id is not  null THEN false ELSE null END) AS fragrance_woody,

-----------------------------------------------------SKIN ISSUES--------------------------------------
                MAX(ifnull(CASE WHEN sq.id = 46256 THEN sa.value END,CASE WHEN sq.id = 15424 THEN sa.value END)) AS skin_complexion,
         MAX(ifnull(CASE WHEN sq.id = 46257 THEN sa.value END,CASE WHEN sq.id = 15425 THEN sa.value END)) AS skin_type,
                 MAX(CASE WHEN sq.id = 46258 and sa.id = 113178 THEN true 
         WHEN  sq.id = 46258  and sa.id is not  null THEN false ELSE null END) AS skin_redness,
         MAX(CASE WHEN sq.id = 46258 and sa.id = 113179 THEN true 
              WHEN  sq.id = 46258  and sa.id is not  null THEN false ELSE null END)AS skin_sensitiveness,
         MAX(CASE WHEN sq.id = 46258 and sa.id = 113180 THEN true 
              WHEN  sq.id = 46258  and sa.id is not  null THEN false ELSE null END) AS skin_aging,
         MAX(CASE WHEN sq.id = 46258 and sa.id = 113181 THEN true 
              WHEN  sq.id = 46258  and sa.id is not  null THEN false ELSE null END)AS skin_acne,
         MAX(CASE WHEN sq.id = 46258 and sa.id = 113182 THEN true 
              WHEN  sq.id = 46258  and sa.id is not  null THEN false ELSE null END) AS skin_dilated_pores,
          MAX(CASE WHEN sq.id = 46258 and sa.id = 113183 THEN true 
         WHEN  sq.id = 46258  and sa.id is not  null THEN false ELSE null END) AS skin_dehydration,
         MAX(CASE WHEN sq.id = 46258 and sa.id = 113184 THEN true 
              WHEN  sq.id = 46258  and sa.id is not  null THEN false ELSE null END)AS skin_eye_bags,
         MAX(CASE WHEN sq.id = 46258 and sa.id = 113185 THEN true 
              WHEN  sq.id = 46258  and sa.id is not  null THEN false ELSE null END) AS skin_dullness,
         MAX(CASE WHEN sq.id = 46258 and sa.id = 113186 THEN true 
              WHEN  sq.id = 46258  and sa.id is not  null THEN false ELSE null END)AS skin_no_problem,
         MAX(CASE WHEN sq.id = 46258 and sa.id = 153850 THEN true 
              WHEN  sq.id = 46258  and sa.id is not  null THEN false ELSE null END) AS skin_spots,
          MAX(CASE WHEN sq.id = 46258 and sa.id = 153851 THEN true 
              WHEN  sq.id = 46258  and sa.id is not  null THEN false ELSE null END) AS skin_wrinkles,
------------------------------------------BODY IssUES----------------------------------

        MAX(CASE WHEN sq.id = 46259 and sa.id = 113187 THEN true 
         WHEN  sq.id = 46259  and sa.id is not  null THEN false ELSE null END) AS body_stretch_marks,
         MAX(CASE WHEN sq.id = 46259 and sa.id = 113188 THEN true 
              WHEN  sq.id = 46259  and sa.id is not  null THEN false ELSE null END)AS body_cellulite,
         MAX(CASE WHEN sq.id = 46259 and sa.id = 113189 THEN true 
              WHEN  sq.id = 46259  and sa.id is not  null THEN false ELSE null END) AS body_lack_firmness,
         MAX(CASE WHEN sq.id = 46259 and sa.id = 113190 THEN true 
              WHEN  sq.id = 46259  and sa.id is not  null THEN false ELSE null END)AS body_dry_skin,
         MAX(CASE WHEN sq.id = 46259 and sa.id = 113191 THEN true 
              WHEN  sq.id = 46259  and sa.id is not  null THEN false ELSE null END) AS body_water_retention,
          MAX(CASE WHEN sq.id = 46259 and sa.id = 113192 THEN true 
         WHEN  sq.id = 46259  and sa.id is not  null THEN false ELSE null END) AS body_no_problem,
         MAX(CASE WHEN sq.id = 46259 and sa.id = 193350 THEN true 
         WHEN  sq.id = 46259  and sa.id is not  null THEN false ELSE null END) AS body_spots,
-------------------hair----------------------------------------------------------------------------------------------------

          MAX(ifnull(CASE WHEN sq.id = 46261 THEN sa.value END,CASE WHEN sq.id = 15422 THEN sa.value END)) AS hair_color,

         MAX(CASE WHEN sq.id = 46845 THEN sa.value END) AS hair_dye,
         MAX(CASE WHEN sq.id = 46846 THEN sa.value END) AS hair_thickness,
         MAX(CASE WHEN sq.id = 46847 THEN sa.value END) AS hair_type,
         MAX(CASE WHEN sq.id = 46848 THEN sa.value END) AS hair_scalp,
         MAX(CASE WHEN sq.id = 46849 THEN sa.value END) AS hair_style,
-------------------hair ISSSUE----------------------------------------------------------------------------------------------------
          MAX(CASE WHEN sq.id = 46850 and sa.id = 114597 THEN true 
         WHEN  sq.id = 46850  and sa.id is not  null THEN false ELSE null END) AS hair_damaged,
          MAX(CASE WHEN sq.id = 46850 and sa.id = 114598 THEN true 
         WHEN  sq.id = 46850  and sa.id is not  null THEN false ELSE null END)AS hair_split_end,
          MAX(CASE WHEN sq.id = 46850 and sa.id = 114599 THEN true 
         WHEN  sq.id = 46850  and sa.id is not  null THEN false ELSE null END) AS hair_greasy,
          MAX(CASE WHEN sq.id = 46850 and sa.id = 114600 THEN true 
         WHEN  sq.id = 46850  and sa.id is not  null THEN false ELSE null END) AS hair_dried,
          MAX(CASE WHEN sq.id = 46850 and sa.id = 114601 THEN true 
         WHEN  sq.id = 46850  and sa.id is not  null THEN false ELSE null END)  AS hair_dandruff,
          MAX(CASE WHEN sq.id = 46850 and sa.id = 114603 THEN true 
         WHEN  sq.id = 46850  and sa.id is not  null THEN false ELSE null END)  AS hair_no_problem,
          MAX(CASE WHEN sq.id = 46850 and sa.id = 153858 THEN true 
         WHEN  sq.id = 46850  and sa.id is not  null THEN false ELSE null END)  AS hair_falls,


      MAX(CASE WHEN sq.id = 46853 THEN sa.value END) AS beauty_routine,
       -- HAIR DREAM : "Mon rêve serait d'avoir les cheveux "
         MAX(CASE WHEN sq.id = 46851 and sa.id = 114604 THEN true 
         WHEN  sq.id = 46851  and sa.id is not  null THEN false ELSE null END) AS want_hair_straight,
         MAX(CASE WHEN sq.id = 46851 and sa.id = 114605 THEN true 
         WHEN  sq.id = 46851  and sa.id is not  null THEN false ELSE null END) AS want_hair_frizz_free,
         MAX(CASE WHEN sq.id = 46851 and sa.id = 114606 THEN true 
         WHEN  sq.id = 46851  and sa.id is not  null THEN false ELSE null END) AS want_hair_volume,
         MAX(CASE WHEN sq.id = 46851 and sa.id = 114607 THEN true 
         WHEN  sq.id = 46851  and sa.id is not  null THEN false ELSE null END)AS want_hair_shine,
         MAX(CASE WHEN sq.id = 46851 and sa.id = 114608 THEN true 
         WHEN  sq.id = 46851  and sa.id is not  null THEN false ELSE null END) AS want_hair_soft,
         MAX(CASE WHEN sq.id = 46851 and sa.id = 114609 THEN true 
         WHEN  sq.id = 46851  and sa.id is not  null THEN false ELSE null END) AS want_hair_less_thinning,
         MAX(CASE WHEN sq.id = 46851 and sa.id = 114610 THEN true 
         WHEN  sq.id = 46851  and sa.id is not  null THEN false ELSE null END) AS want_hair_curly,
         MAX(CASE WHEN sq.id = 46851 and sa.id = 114611 THEN true 
         WHEN  sq.id = 46851  and sa.id is not  null THEN false ELSE null END) AS want_hair_grow,

     
       -- HAIR DRYER USER : "De manière fréquente j'utilise "
         MAX(CASE WHEN sq.id = 46852 and sa.id = 114612 THEN true 
         WHEN  sq.id = 46852  and sa.id is not  null THEN false ELSE null END)AS use_hair_dryer,
         MAX(CASE WHEN sq.id = 46852 and sa.id = 114613 THEN true 
         WHEN  sq.id = 46852  and sa.id is not  null THEN false ELSE null END) AS use_hair_straightener,
         MAX(CASE WHEN sq.id = 46852 and sa.id = 114614 THEN true 
         WHEN  sq.id = 46852  and sa.id is not  null THEN false ELSE null END) AS use_hair_no_device,
          max(sra._rivery_last_update) last_update

  FROM inter.survey_questions sq
  INNER JOIN inter.survey_answers sa ON COALESCE(sq.parent_id, sq.id) = sa.question_id AND sa.dw_country_code = 'FR'
  INNER JOIN inter.survey_results sr ON sq.dw_country_code = sr.dw_country_code AND sq.survey_id = sr.survey_id
  INNER JOIN inter.survey_result_answers sra ON sra.dw_country_code = sq.dw_country_code AND sra.question_id = sq.id AND sra.result_id = sr.id AND sra.answer_id = sa.id
  WHERE sq.survey_id = 2639 --and user_id=2622634 -- and sq.id=46259
  {% if is_incremental() %}
        AND sra._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
    {% endif %}
  group by all
 