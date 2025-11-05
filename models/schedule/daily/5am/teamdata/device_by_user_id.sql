SELECT distinct user_id,Device_ID FROM `teamdata-291012.cdpimagino.BQ_imagino_Tag_Event_v2` 
where user_id is not null and length(user_id)>3