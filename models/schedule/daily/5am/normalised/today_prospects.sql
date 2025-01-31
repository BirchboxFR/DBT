select distinct user_id,last_box_paid_date,box_sub_status_lvl1 as status,'PROSPECTS' as status
 from user.customers 
where (date_diff(current_date(),last_box_paid_date,month) >36 
or
last_box_paid_date is null

 )
and box_sub_status in ('CHURN','NEVERSUB')


