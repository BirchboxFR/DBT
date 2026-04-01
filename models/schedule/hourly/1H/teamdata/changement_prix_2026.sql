
SELECT user_id, user_key, max(t.total_boxes_so_far) AS total_boxes_so_far, max(economie) AS economie, email, firstname, lastname, initial_box_date, max(payment_status) AS payment_status, 
initial_box_month, initial_box_year, MAX(sub_id) AS sub_id
FROM
(
select bs.user_id,bs.user_key,bs.total_boxes_so_far,
FLOOR(bs.total_boxes_so_far * 59.10 / 10) * 10 economie,
email,firstname,lastname,initial_box_date,bs.payment_Status,
initial_box_month,
initial_box_year,
bs1.sub_id
FROM sales.box_sales bs
join user.customers using(user_key)
JOIN sales.box_sales bs1 ON bs1.user_key = bs.user_key AND bs1.box_id = bs.box_id + 1
where bs.dw_country_code='FR'
and bs.month=4 and bs.year=2026 and bs.payment_status='paid'
UNION ALL

SELECT c.user_id,c.user_key,c.nb_box_paid AS total_boxes_so_far,
FLOOR(c.nb_box_paid * 59.10 / 10) * 10 economie,
c.email,c.firstname,c.lastname,c.initial_box_date,'pause' AS payment_Status,
c.initial_box_month,
c.initial_box_year,
bs1.sub_id
FROM `teamdata-291012.sales.box_paused` bp
JOIN user.customers c USING (user_id, dw_country_code)
LEFT JOIN sales.box_sales bs USING(user_id, date, dw_country_code)
JOIN sales.box_sales bs1 ON bs1.user_key = c.user_key AND bs1.box_id = bp.box_id + 1
WHERE bp.date = '2026-04-01'
AND bp.end_box_of_sequence = 171
AND bp.dw_country_code = 'FR'
AND bs.sub_id IS NULL
) t
GROUP BY ALL