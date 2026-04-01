select bs.user_id,user_key,total_boxes_so_far,
FLOOR(total_boxes_so_far * 59.10 / 10) * 10 economie,
email,firstname,lastname,initial_box_date,payment_Status,
initial_box_month,
initial_box_year,
 from sales.box_sales bs
join user.customers using(user_key)
where bs.dw_country_code='FR'
and month=4 and year=2026 and payment_status='paid'