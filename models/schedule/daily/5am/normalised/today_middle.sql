

select distinct t.user_id,'middle' as status,box_sub_status as status_lvl1 
from teamdata-291012.user.customers t
left join `normalised-417010`.`user`.`today_prospects` p on p.user_id=t.user_id 
left join `normalised-417010`.`user`.`today_spectators` sp on sp.user_id=t.user_id 
left join `normalised-417010`.`user`.`today_new` n on n.user_id=t.user_id 
left join `normalised-417010`.`user`.`today_whales` w on w.user_id=t.user_id 
left join `normalised-417010`.`user`.`today_stars` stars on stars.user_id=t.user_id 
left join `normalised-417010`.`user`.`today_risky` ri on ri.user_id=t.user_id 
left join `normalised-417010`.`user`.`today_inactive` i on i.user_id=t.user_id 
left join `normalised-417010`.`user`.`today_lost` l on l.user_id=t.user_id 
where p.user_id is null and sp.user_id is null and n.user_id is null and w.user_id is null and stars.user_id is null and ri.user_id is null
and i.user_id is null and l.user_id is null

