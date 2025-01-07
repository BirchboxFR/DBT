SELECT name, price,
       date_start,
       DATE_SUB(LEAD(date_start) OVER (PARTITION BY name ORDER BY date_start), INTERVAL 1 DAY) AS date_end
FROM `update_table.logistics_costs`
WHERE name IS NOT NULL
