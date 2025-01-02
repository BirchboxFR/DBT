WITH
sub_suspend_survey_reason AS (
    SELECT * EXCEPT (row_num)
    FROM
        (
            SELECT
                sr.dw_country_code,
                sr.customer_id AS user_id,
                b.id AS box_id,
                sqa.title AS survey_reason,
                ROW_NUMBER()
                    OVER (
                        PARTITION BY sr.dw_country_code, sr.customer_id, b.id
                        ORDER BY sr.answered_at DESC
                    )
                    AS row_num
            FROM `teamdata-291012.inter.sub_suspend_survey_result` AS sr
            INNER JOIN
                `teamdata-291012.inter.boxes` AS b
                ON
                    sr.dw_country_code = b.dw_country_code
                    AND b.id = sr.last_received_box_id + 1
            INNER JOIN
                `teamdata-291012.inter.sub_suspend_survey_result_answer` AS sra
                ON
                    sr.result_id = sra.result_id
                    AND sr.dw_country_code = sra.dw_country_code
            INNER JOIN
                `teamdata-291012.bdd_prod_fr.wp_jb_sub_suspend_survey_question_answer`
                    AS sqa
                ON sra.question_answer_id = sqa.question_answer_id
            WHERE sra.question_id = 1
        ) AS t
    WHERE row_num = 1
),

products AS (
    SELECT
        box_id,
        coffret_id,
        dw_country_code,
        product_codification_id,
        MAX(inventory_item_id) AS inventory_item_id,
        MAX(id) AS id
    FROM inter.products

    GROUP BY ALL
),

shipping_mode_dedup AS (
    SELECT
        shipping_mode_id,
        min_weight,
        max_weight,
        date_start,
        date_end,
        MAX(price) AS price,
        MAX(price_daily) AS price_daily,
        MAX(shipping_taxes_rate) AS shipping_taxes_rate
    FROM ops.shipping_costs
    GROUP BY ALL
),

ranked_sub_history AS (
    SELECT
        o.dw_country_code,
        o.user_id,
        sh.box_id,
        'reason from survey' AS sub_suspended_reason_lvl3,
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', sh.timestamp) AS d,
        CASE
            WHEN
                ssr.value IN ('Too many fails', 'Card expired', 'Breakage')
                THEN 'technical'
            WHEN
                ssr.value IN ('Self suspended', 'Paused', 'Paused for gift')
                THEN 'self-willed'
        END AS sub_suspended_reason_lvl1,
        CASE
            WHEN ssr.value = 'Card expired' THEN 'expired card'
            WHEN ssr.value IN ('Self suspended') THEN 'suspended'
            WHEN ssr.value IN ('Paused', 'Paused for gift') THEN 'paused'
            WHEN ssr.value IN ('Too many fails', 'Breakage') THEN 'breakage'
        END AS sub_suspended_reason_lvl2,
        ROW_NUMBER()
            OVER (
                PARTITION BY o.user_id, sh.box_id, sh.dw_country_code
                ORDER BY timestamp DESC
            )
            AS row_num
    FROM inter.sub_history AS sh
    INNER JOIN
        inter.order_detail_sub AS s
        ON
            sh.order_detail_id = s.order_detail_id
            AND sh.box_id = s.box_id
            AND sh.dw_country_code = s.dw_country_code
    INNER JOIN
        inter.order_details AS d
        ON s.order_detail_id = d.id AND s.dw_country_code = d.dw_country_code
    INNER JOIN
        inter.orders AS o
        ON d.order_id = o.id AND d.dw_country_code = o.dw_country_code
    INNER JOIN
        `inter.sub_suspended_reasons`
            AS ssr
        ON
            sh.dw_country_code = ssr.dw_country_code
            AND sh.sub_suspended_reasons_id = ssr.id


            AND sh.action = -1

),

sub_history_reasons AS (
    SELECT * EXCEPT (row_num)
    FROM ranked_sub_history
    WHERE row_num = 1
),

adyen_ranked AS (
    SELECT
        an.dw_country_code,
        o.user_id,
        s.box_id,
        'technical' AS sub_suspended_reason_lvl1,
        an.reason AS sub_suspended_reason_lvl3,
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', an.eventdate) AS d,
        CASE
            WHEN an.reason LIKE '%xpired%' THEN 'expired card' ELSE 'breakage'
        END AS sub_suspended_reason_lvl2,
        ROW_NUMBER()
            OVER (
                PARTITION BY o.user_id, s.box_id, s.dw_country_code
                ORDER BY an.eventdate DESC
            )
            AS row_num
    FROM `inter.adyen_notifications` AS an
    INNER JOIN
        inter.order_detail_sub AS s
        ON an.sub_id = s.id AND an.dw_country_code = s.dw_country_code
    INNER JOIN
        inter.order_details AS d
        ON s.order_detail_id = d.id AND s.dw_country_code = d.dw_country_code
    INNER JOIN
        inter.orders AS o
        ON d.order_id = o.id AND d.dw_country_code = o.dw_country_code
    WHERE an.success = 0

),

adyen_reasons AS (
    SELECT * EXCEPT (row_num)
    FROM adyen_ranked
    WHERE row_num = 1
),

all_reasons AS (
    SELECT sr.*
    FROM sub_history_reasons AS sr
    LEFT JOIN
        adyen_reasons AS an
        ON
            sr.dw_country_code = an.dw_country_code
            AND sr.user_id = an.user_id
            AND sr.box_id = an.box_id
    WHERE an.user_id IS NULL
    UNION ALL
    SELECT *
    FROM adyen_reasons
),

all_reasons_ranked AS (
    SELECT
        all_reasons.dw_country_code,
        all_reasons.user_id,
        all_reasons.box_id,
        all_reasons.sub_suspended_reason_lvl1,
        all_reasons.sub_suspended_reason_lvl2,
        CASE
            WHEN
                sub_suspended_reason_lvl3 = 'reason from survey'
                THEN ssr.survey_reason
            ELSE sub_suspended_reason_lvl3
        END AS sub_suspended_reason_lvl3,
        ROW_NUMBER()
            OVER (
                PARTITION BY
                    all_reasons.user_id,
                    all_reasons.box_id,
                    all_reasons.dw_country_code
                ORDER BY d DESC
            )
            AS row_num
    FROM all_reasons
    LEFT JOIN
        sub_suspend_survey_reason AS ssr
        ON
            all_reasons.dw_country_code = ssr.dw_country_code
            AND all_reasons.user_id = ssr.user_id
            AND all_reasons.box_id = ssr.box_id
),

self_churn_reason AS (
    SELECT * EXCEPT (row_num)
    FROM all_reasons_ranked
    WHERE row_num = 1
),

gws_costs_table AS (
    SELECT
        sol.dw_country_code,
        sol.sub_id,
        COALESCE(SUM(c.purchase_price * d.quantity), 0) AS gws_costs
    FROM inter.sub_order_link AS sol
    INNER JOIN
        inter.orders AS o
        ON sol.dw_country_code = o.dw_country_code AND sol.order_id = o.id
    INNER JOIN
        inter.order_details AS d
        ON o.dw_country_code = d.dw_country_code AND o.id = d.order_id
    INNER JOIN
        {{ ref('catalog') }} AS c
        ON d.dw_country_code = c.dw_country_code AND d.product_id = c.product_id
    WHERE d.special_type = 'GWS' AND status = 1
    GROUP BY
        sol.dw_country_code,
        sol.sub_id
),

box_global_grades AS (
    SELECT
        p.dw_country_code,
        p.box_id,
        p.coffret_id,
        MAX(global_grade) AS global_grade
    FROM `teamdata-291012.Spreadsheet_synchro.raw_doc_compo` AS c
    INNER JOIN inter.products AS p ON c.sku_compo = p.sku
    GROUP BY p.dw_country_code, p.box_id, p.coffret_id
)

SELECT
    t.*,
    t.box_id + 1 AS next_month_id,
    LAG(t.date)
        OVER (PARTITION BY t.user_id, t.dw_country_code ORDER BY t.box_id)
        AS last_box_received_date,
    CASE
        WHEN
            -- next box by user is the next box
            LEAD(t.box_id)
                OVER (
                    PARTITION BY t.user_id, t.dw_country_code ORDER BY t.box_id
                )
            - t.box_id IN (0, 1)
            -- next box in the subscription (by order_detail)
            OR LEAD(t.box_id)
                OVER (
                    PARTITION BY t.order_detail_id, t.dw_country_code
                    ORDER BY t.box_id
                )
            - t.box_id
            = 1
            THEN 'LIVE'
        ELSE 'CHURN'
    END AS next_month_status,

    CASE
        WHEN
            LEAD(t.box_id)
                OVER (
                    PARTITION BY t.user_id, t.dw_country_code ORDER BY t.box_id
                )
            - t.box_id IN (0, 1)
            OR LEAD(t.box_id)
                OVER (
                    PARTITION BY t.order_detail_id, t.dw_country_code
                    ORDER BY t.box_id
                )
            - t.box_id
            = 1
            THEN NULL
        WHEN t.gift = 1 THEN 'gift end'
        ELSE COALESCE(scr.sub_suspended_reason_lvl1, 'self-willed')
    END
        AS sub_suspended_reason_lvl1,
    CASE
        WHEN
            LEAD(t.box_id)
                OVER (
                    PARTITION BY t.user_id, t.dw_country_code ORDER BY t.box_id
                )
            - t.box_id IN (0, 1)
            OR LEAD(t.box_id)
                OVER (
                    PARTITION BY t.order_detail_id, t.dw_country_code
                    ORDER BY t.box_id
                )
            - t.box_id
            = 1
            THEN NULL
        WHEN t.gift = 1 THEN 'gift end'
        ELSE COALESCE(scr.sub_suspended_reason_lvl2, 'suspended')
    END
        AS sub_suspended_reason_lvl2,
    CASE
        WHEN
            LEAD(t.box_id)
                OVER (
                    PARTITION BY t.user_id, t.dw_country_code ORDER BY t.box_id
                )
            - t.box_id IN (0, 1)
            OR LEAD(t.box_id)
                OVER (
                    PARTITION BY t.order_detail_id, t.dw_country_code
                    ORDER BY t.box_id
                )
            - t.box_id
            = 1
            THEN NULL
        WHEN t.gift = 1 THEN 'gift end'
        ELSE COALESCE(scr.sub_suspended_reason_lvl3, 'suspended')
    END
        AS sub_suspended_reason_lvl3,
    bgg.global_grade AS box_global_grade,


    mcd.type AS coupon_type,   -- new coupon_typet.box_id
    CASE WHEN
        t.box_id
        - LAG(t.box_id)
            OVER (
                PARTITION BY t.user_id, t.dw_country_code
                ORDER BY t.box_id, t.order_detail_id
            )
        IN (0, 1)
        OR
        t.box_id
        - LAG(t.box_id)
            OVER (
                PARTITION BY t.order_detail_id, t.dw_country_code
                ORDER BY t.box_id, t.order_detail_id
            )
        = 1
        THEN 'LIVE'
    ELSE 'ACQUISITION' END AS acquis_status_lvl1,
    CASE WHEN
        t.box_id
        - LAG(t.box_id)
            OVER (
                PARTITION BY t.user_id, t.dw_country_code
                ORDER BY t.box_id, t.order_detail_id
            )
        IN (0, 1)
        OR
        t.box_id
        - LAG(t.box_id)
            OVER (
                PARTITION BY t.order_detail_id, t.dw_country_code
                ORDER BY t.box_id, t.order_detail_id
            )
        = 1
        THEN 'LIVE'
    WHEN t.gift = 1 THEN 'GIFT'
    WHEN
        LAG(t.box_id)
            OVER (PARTITION BY t.user_id, t.dw_country_code ORDER BY t.box_id)
        IS NOT NULL
        THEN 'REACTIVATION'
    WHEN
        LAG(t.box_id)
            OVER (PARTITION BY t.user_id, t.dw_country_code ORDER BY t.box_id)
        IS NULL
        THEN 'NEW NEW'
    ELSE 'Unknown' END AS acquis_status_lvl2,
    CASE WHEN cannot_suspend = 1 THEN 'committed' ELSE 'not committed' END
        AS committed,
    g.gws_costs,
    t.total_product / (1 + vat_rate / 100) AS gross_revenue,
    t.total_product
    - t.total_product / (1 + vat_rate / 100) AS vat_on_gross_revenue,
    t.total_discount / (1 + vat_rate / 100) AS discount,
    t.total_discount
    - t.total_discount / (1 + vat_rate / 100) AS vat_on_discount,
    t.total_product / (1 + vat_rate / 100)
    - (t.total_discount / (1 + vat_rate / 100)) AS net_revenue,
    t.total_shipping / (1 + vat_rate / 100) AS shipping,
    t.total_shipping
    - t.total_shipping / (1 + vat_rate / 100) AS vat_on_shipping,
    t.total_product / (1 + vat_rate / 100)
    - (t.total_discount / (1 + vat_rate / 100))
    - (
        t.coop
        + t.assembly_cost
        + t.pack_cost
        + t.print_cost
        + t.consumable_cost
        + t.shipping_cost
        + COALESCE(g.gws_costs, 0)
    ) AS gross_profit
FROM
    (
        SELECT
            o.dw_country_code,
            s.id AS sub_id,
            d.id AS order_detail_id,
            o.id AS order_id,
            o.user_id,
            s.box_id,
            s.coffret_id,
            b.date,
            o.coupon_code_id,
            s.sub_offer_id,
            s.reactivated_date,
            s.shipping_mode,
            d.quantity AS dquantity,
            s.cannot_suspend,
            o.dw_country_code AS store_code,
            s.shipping_country,
            sps.name AS sub_payment_status,
            sub_payment_status_id,
            d.sub_start_box,
            s.shipping_firstname,
            s.shipping_lastname,
            d.gift_card_id,
            kc.coop,
            kc.assembly_cost,
            kc.pack_cost,
            kc.print_cost,
            kc.consumable_cost,
            sc.price AS shipping_cost,
            b1.date AS next_month_date,
            s.next_payment_date,
            s.last_payment_date,
            COALESCE (s.box_id = current_box_id, FALSE)
                AS is_current,
            b.id - cbt.current_box_id AS diff_current_box,
            DATE_DIFF(
                COALESCE (an.eventdate,
                s.last_payment_date),
                b.shipping_date,
                DAY
            )
            + 1 AS day_in_cycle,
            COALESCE (an.eventdate, s.last_payment_date) AS payment_date,
            DATE_DIFF(
                COALESCE (an.eventdate,
                s.last_payment_date),
                b.shipping_date,
                DAY
            )
            + 1 AS nb_days_since_opening,
            DATE_DIFF(
                COALESCE (an.eventdate,
                s.last_payment_date),
                b1.shipping_date,
                DAY
            ) AS nb_days_next_cycle,
            EXTRACT(MONTH FROM b.date) AS month,

            EXTRACT(YEAR FROM b.date) AS year,
            COALESCE(coupons_parents.code, c.code) AS coupon_code,
            COALESCE(so_parents.code, so.code) AS sub_offer_code,
            CASE
                WHEN
                    s.box_id = d.sub_start_box
                    THEN COALESCE(coupons_parents.code, c.code)
                ELSE COALESCE(so_parents.code, so.code)
            END AS coupon,
            CASE
                WHEN d.gift_card_id = 0 THEN 1
                WHEN bg.gift IS NULL THEN 1
                WHEN bg.gift = 1 THEN 0
                WHEN bg.gift = 0 THEN 1
                ELSE 1
            END AS self,
            CASE
                WHEN bg.gift IS NULL THEN 0
                WHEN bg.gift = 1 THEN 1
                WHEN bg.gift = 0 THEN 0
                ELSE bg.gift
            END AS gift,
            CASE
                WHEN
                    yc.yearly_coupon_id IS NOT NULL AND s.cannot_suspend = 1
                    THEN 1
                ELSE 0
            END AS yearly,
            CASE WHEN d.quantity = -12 THEN 1 ELSE 0 END AS old_yearly,
            CASE
                WHEN s.sub_payment_status_id = 8 OR o.status_id = 3 THEN 0
                WHEN s.total_product = 0 AND gc.id IS NULL THEN 0
                WHEN
                    s.total_product = 0
                    AND gc.id IS NOT NULL
                    AND pbp.sub_id IS NULL
                    THEN b.box_quantity * (gc.amount / gc.duration)
                -- if partial box paid, count only one box
                WHEN
                    s.total_product = 0
                    AND gc.id IS NOT NULL
                    AND pbp.sub_id IS NOT NULL
                    THEN (gc.amount / gc.duration)
                ELSE s.total_product
            END AS total_product,
            COALESCE(tva.taux, 0) AS vat_rate,
            CASE
                WHEN s.sub_payment_status_id = 8 OR o.status_id = 3 THEN 0.0
                -- Veepee offer - May 2021
                WHEN
                    c.parent_id = 15237671 AND s.box_id = d.sub_start_box
                    THEN 0.0
                -- Veepee offer - May 2021
                WHEN so.parent_offer_id = 53382 THEN 0.0
                ELSE s.total_discount
            END AS total_discount,
            CASE
                WHEN
                    s.sub_payment_status_id = 8 OR o.status_id = 3
                    THEN 0.0
                ELSE s.total_shipping
            END AS total_shipping,
            CASE
                WHEN s.sub_payment_status_id = 3 THEN 'forthcoming' ELSE 'paid'
            END AS payment_status,
            CASE WHEN o.raf_parent_id > 0 THEN 1 ELSE 0 END AS raffed,
            CASE
                WHEN
                    (c.discount_type = 'PRODUCT' AND d.sub_start_box = s.box_id)
                    OR so.offer_type = 'PRODUCT'
                    THEN 'GWS'
                WHEN
                    (
                        c.discount_type IN (
                            'CURRENCY', 'PERCENT', 'CURRENCY_TOTAL'
                        )
                        AND d.sub_start_box = s.box_id
                    )
                    OR so.offer_type IN (
                        'CURRENCY', 'PERCENT', 'CURRENCY_TOTAL'
                    )
                    THEN 'discount'
                ELSE 'Other'
            END AS discount_type,
            CASE
                WHEN
                    (c.sub_engagement_period > 0 AND d.sub_start_box = s.box_id)
                    OR so.sub_engagement_period > 0
                    THEN 'engaged'
                ELSE 'not engaged'
            END AS coupon_engagement,
            CASE
                WHEN
                    s.cannot_suspend = 1
                    AND (
                        LEAD(s.cannot_suspend)
                            OVER (
                                PARTITION BY
                                    s.order_detail_id, s.dw_country_code
                                ORDER BY s.box_id
                            )
                        = 0
                        OR LEAD(s.cannot_suspend)
                            OVER (
                                PARTITION BY
                                    s.order_detail_id, s.dw_country_code
                                ORDER BY s.box_id
                            )
                        IS NULL
                    )

                    THEN 1
                ELSE 0
            END AS last_committed_box
        -- sub_suspended_reason_lvl1,sub_suspended_reason_lvl2,sub_suspended_reason_lvl3
        FROM inter.orders AS o
        INNER JOIN
            inter.order_details AS d
            ON o.id = d.order_id AND o.dw_country_code = d.dw_country_code
        INNER JOIN
            inter.order_detail_sub AS s
            ON
                d.id = s.order_detail_id
                AND d.dw_country_code = s.dw_country_code
        INNER JOIN
            inter.boxes AS b
            ON s.box_id = b.id AND s.dw_country_code = b.dw_country_code
        INNER JOIN
            inter.boxes AS b1
            ON b1.id = s.box_id + 1 AND s.dw_country_code = b1.dw_country_code
        INNER JOIN
            bdd_prod_fr.wp_jb_sub_payments_status AS sps
            ON s.sub_payment_status_id = sps.id
        INNER JOIN
            snippets.current_box AS cbt
            ON o.dw_country_code = cbt.dw_country_code
        LEFT JOIN
            products AS p
            ON
                o.dw_country_code = p.dw_country_code
                AND b.id = p.box_id
                AND s.coffret_id = p.coffret_id
                AND p.product_codification_id = 29
        LEFT JOIN
            product.kit_costs AS kc
            ON
                o.dw_country_code = kc.country_code
                AND p.inventory_item_id = kc.inventory_item_id
                AND p.id = kc.kit_id
        LEFT JOIN
            shipping_mode_dedup AS sc
            ON
                b.date >= sc.date_start
                AND (b.date <= sc.date_end OR sc.date_end IS NULL)
                AND s.shipping_mode = sc.shipping_mode_id
                AND CASE
                    WHEN b.box_quantity = 1 THEN 0.4 WHEN
                        b.box_quantity = 2
                        THEN 0.8
                END
                >= min_weight
                AND (
                    CASE
                        WHEN b.box_quantity = 1 THEN 0.4 WHEN
                            b.box_quantity = 2
                            THEN 0.8
                    END
                    < max_weight
                    OR max_weight IS NULL
                )
        LEFT JOIN
            inter.gift_cards AS gc
            ON d.gift_card_id = gc.id AND d.dw_country_code = gc.dw_country_code
        LEFT JOIN
            payment.adyen_notifications_authorization AS an
            ON s.id = an.sub_id AND s.dw_country_code = an.dw_country_code
        LEFT JOIN
            inter.coupons AS c
            ON o.coupon_code_id = c.id AND o.dw_country_code = c.dw_country_code
        LEFT JOIN
            inter.coupons AS coupons_parents
            ON
                c.parent_id = coupons_parents.id
                AND c.dw_country_code = coupons_parents.dw_country_code
        LEFT JOIN
            inter.sub_offers AS so
            ON s.sub_offer_id = so.id AND s.dw_country_code = so.dw_country_code
        LEFT JOIN
            inter.sub_offers AS so_parents
            ON
                so.parent_offer_id = so_parents.id
                AND so.dw_country_code = so_parents.dw_country_code
        LEFT JOIN
            inter.tva_product AS tva
            ON
                s.shipping_country = tva.country_code
                AND tva.category = 'normal'
                AND s.dw_country_code = tva.dw_country_code
        LEFT JOIN
            sales.box_gift AS bg
            ON s.dw_country_code = bg.dw_country_code AND s.id = bg.sub_id
        LEFT JOIN
            snippets.yearly_coupons AS yc
            ON
                o.dw_country_code = yc.country_code
                AND o.coupon_code_id = yc.yearly_coupon_id
        /*LEFT JOIN (select user_id,month,year,dw_country_code,box_id,max(sub_suspended_reason_lvl1)sub_suspended_reason_lvl1,max(sub_suspended_reason_lvl2)sub_suspended_reason_lvl2,max(sub_suspended_reason_lvl3)sub_suspended_reason_lvl3 from`teamdata-291012.sales.box_sales_by_user_by_type`  group by 1,2,3,4,5)bsbu ON o.dw_country_code = bsbu.dw_country_code AND bsbu.user_id=o.user_id and bsbu.box_id = s.box_id + 1*/
        LEFT JOIN
            `inter.partial_box_paid` AS pbp
            ON s.dw_country_code = pbp.dw_country_code AND s.id = pbp.sub_id
        WHERE -- o.status_id IN (1, 3) AND 
            (
                s.shipping_status_id IN (2, 3, 4, 5, 19, 22)
                OR (
                    s.sub_payment_status_id = 3
                    AND s.box_id >= cbt.current_box_id
                )
            )
            AND s.box_id <= cbt.current_box_id + 36


    ) AS t
LEFT JOIN
    gws_costs_table AS g
    ON t.dw_country_code = g.dw_country_code AND t.sub_id = g.sub_id
LEFT JOIN (
SELECT DISTINCT
    country,
    code,
    MAX(type) AS type,
    MAX(date) AS date,
    MAX(type2) AS type2,
    MAX(coupon_id) AS coupon_id,
    MAX(sub_offer_id) AS sub_offer_id
FROM `teamdata-291012.marketing.Marketing_cac_discount`
GROUP BY 1, 2)
AS mcd
ON mcd.coupon_id = coupon_code_id AND t.dw_country_code = mcd.country
LEFT JOIN
(
    SELECT DISTINCT
        country,
        code,
        MAX(type) AS type,
        MAX(date) AS date,
        MAX(type2) AS type2,
        MAX(coupon_id) AS coupon_id,
        MAX(sub_offer_id) AS sub_offer_id
    FROM `teamdata-291012.marketing.Marketing_cac_discount`
    GROUP BY 1, 2
) AS mcdso
ON t.sub_offer_id = mcdso.sub_offer_id AND t.dw_country_code = mcd.country
LEFT JOIN
box_global_grades AS bgg
ON
    t.dw_country_code = bgg.dw_country_code
    AND t.box_id = bgg.box_id
    AND t.coffret_id = bgg.coffret_id
LEFT JOIN
self_churn_reason AS scr
ON
    t.dw_country_code = scr.dw_country_code
    AND t.user_id = scr.user_id
    AND scr.box_id = t.box_id + 1

