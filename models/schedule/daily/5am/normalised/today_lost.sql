select distinct user_id,last_box_paid_date,box_sub_status as status_lvl1,'LOST' as status,last_shop_order_date,last_box_paid_date
 from user.customers 
where (date_diff(current_date(),last_box_paid_date,month)between 14 and 24 
and ( 
is_shopper =false or date_diff(current_date(),last_shop_order_date,month)between 14 and 24 

)

 )
and box_sub_status in ('CHURN','NEVERSUB')


