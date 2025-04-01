select distinct user_id,status

from (
select distinct user_id,status
from {{ref('today_stars') }}
union all
select user_id,status from {{ ref('today_whales') }}
union all
select user_id,status from {{ ref('today_middle') }}
union all
select user_id,status from {{ ref('today_new') }}
union all
select user_id,status from {{ ref('today_lost') }}
union all
select user_id,status from {{ ref('today_prospects') }}
union all
select user_id,status from {{ ref('today_inactive') }}
union all
select user_id,status from {{ ref('today_risky') }}
union all
select user_id,status from {{ ref('today_spectators') }}
)
where user_id=2342637
