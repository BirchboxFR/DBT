-- Aggregate tracking by Campaign and Message Id
WITH Tracking AS ( 
	SELECT 
		 t.activationId
		,t.messageId
		,MAX(CASE WHEN t.type ="open" THEN t.eventDate ELSE null END) AS LAST_OPEN_DATE
		,MIN(CASE WHEN t.type ="open" THEN t.eventDate ELSE null END) AS FIRST_OPEN_DATE
		,MAX(CASE WHEN t.type ="click" AND t.url NOT LIKE "%unsub%" THEN t.eventDate ELSE null END) AS LAST_CLICK_DATE
		,MIN(CASE WHEN t.type ="click" AND t.url NOT LIKE "%unsub%"THEN t.eventDate ELSE null END) AS FIRST_CLICK_DATE
		,MAX(CASE WHEN t.type ="click" AND t.url LIKE "%unsub%" THEN t.eventDate ELSE null END) AS LAST_UNSUB_DATE
		,MIN(CASE WHEN t.type ="click" AND t.url LIKE "%unsub%"THEN t.eventDate ELSE null END) AS FIRST_UNSUB_DATE
	FROM cdpimagino.imaginoreplicatedtracking t
	WHERE 
		t.activationId = 'FR_Welcome_Jj_SansAchat'
	GROUP BY ALL
)

SELECT 
	FORMAT_DATETIME('%F', m.eventdate) AS EVENTDATE -- Format date YYYY-MM-DD
	,m.activationId
	,COUNT(distinct id) AS Targeted_WithIgnored
	,SUM( CASE WHEN m.status != "ignored"  THEN 1 ELSE 0 END) AS TARGETED_withoutIgnored
	,SUM( CASE WHEN m.status = "delivered"  THEN 1 ELSE 0 END) AS DELIVERED
	,SUM( CASE WHEN t.LAST_OPEN_DATE IS NOT NULL THEN 1 ELSE 0 END) AS DISCTINT_OPEN
	,SUM( CASE WHEN t.LAST_CLICK_DATE IS NOT NULL THEN 1 ELSE 0 END) AS DISCTINT_CLICK
	,SUM( CASE WHEN t.LAST_UNSUB_DATE IS NOT NULL THEN 1 ELSE 0 END) AS DISCTINT_UNSUB
FROM cdpimagino.imaginoreplicatedmessage m
LEFT JOIN Tracking t ON 
	t.messageId = m.id
	AND t.activationId = m.activationId
WHERE m.activationId = 'FR_Welcome_Jj_SansAchat'
	AND m.address != ""
	--AND DATE (m.eventdate) = '2026-01-05'
	--AND m.contactData LIKE "%journeyId%"
GROUP BY ALL
order by eventDate desc