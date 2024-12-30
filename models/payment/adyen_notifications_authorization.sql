SELECT MAX(value) AS value,
       MIN(eventDate) AS eventDate,
       dw_country_code,
       eventCode,
       order_id,
       detail_id,
       sub_id
FROM inter.adyen_notifications an
WHERE success = 1
AND eventCode = 'AUTHORISATION'
GROUP BY dw_country_code,
         eventCode,
         order_id,
         detail_id,
         sub_id
