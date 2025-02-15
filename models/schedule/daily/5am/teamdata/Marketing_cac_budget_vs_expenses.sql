SELECT distinct month,upper(b.levier) levier,upper(b.levierFC) levierFC,budget,sum(spent) spent ,safe_divide(sum(spent)  , budget) as ratio_spent
FROM `teamdata-291012.marketing.Marketing_cac_budget` b 
left join {{ ref('Marketing_cac_expenses') }} e on upper(e.levier)=upper(b.levier) and upper(e.levierFC)=upper(b.levierFC) and e.country=b.country and e.mois=b.Month
where b.month>='2023-10-01' and b.country='FR'
group by 1,2,3,4