SELECT distinct user_id as user_key,Device_ID FROM `teamdata-291012.cdpimagino.BQ_imagino_Tag_Event_v2` 
where user_id is not null and length(user_id)>3
and not starts_with(user_id,'_')