SELECT user_id, 
       gp_tot
FROM (
  SELECT user_id, 
         SUM(gp) AS gp_tot
  FROM (
    SELECT user_id, 
           SUM(gross_profit) AS gp
    FROM sales.box_sales
    WHERE diff_current_box BETWEEN -12 AND 0
      AND dw_country_code = 'FR'
    GROUP BY user_id
    UNION ALL
    SELECT user_id, 
           SUM(gross_profit) AS gp
    FROM `teamdata-291012.sales.shop_orders_margin`
    WHERE DATE_DIFF(CURRENT_DATE, order_date, MONTH) BETWEEN -12 AND 0
      AND dw_country_code = 'FR'
    GROUP BY user_id
  )
  GROUP BY user_id
)
qualify NTILE(100) OVER (ORDER BY gp_tot DESC) between 5 and 30
order by gp_tot asc