
SELECT user_id, user_key, max(t.total_boxes_so_far) AS total_boxes_so_far, max(economie) AS economie, email, firstname, lastname, initial_box_date, max(payment_status) AS payment_status, 
initial_box_month, initial_box_year
FROM
(
select bs.user_id,user_key,total_boxes_so_far,
FLOOR(total_boxes_so_far * 59.10 / 10) * 10 economie,
email,firstname,lastname,initial_box_date,payment_Status,
initial_box_month,
initial_box_year,
 from sales.box_sales bs
join user.customers using(user_key)
where bs.dw_country_code='FR'
and month=4 and year=2026 and payment_status='paid'
UNION ALL

SELECT c.user_id,c.user_key,c.nb_box_paid AS total_boxes_so_far,
FLOOR(c.nb_box_paid * 59.10 / 10) * 10 economie,
c.email,c.firstname,c.lastname,c.initial_box_date,'pause' AS payment_Status,
c.initial_box_month,
c.initial_box_year
FROM `teamdata-291012.sales.box_paused` bp
JOIN user.customers c USING (user_id, dw_country_code)
LEFT JOIN sales.box_sales bs USING(user_id, date, dw_country_code)
WHERE bp.date = '2026-04-01'
AND bp.end_box_of_sequence = 171
AND bp.dw_country_code = 'FR'
AND bs.sub_id IS NULL
) t
GROUP BY ALL