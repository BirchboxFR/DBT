WITH 
box_paused AS (
    SELECT o.dw_country_code, o.user_id, s.box_id, b.date
FROM {{ ref('orders') }} o
INNER JOIN {{ ref('order_details') }} d ON o.id = d.order_id AND o.dw_country_code = d.dw_country_code
INNER JOIN {{ ref('order_detail_sub') }} s ON s.order_detail_id = d.id AND s.dw_country_code = d.dw_country_code
INNER JOIN {{ ref('boxes') }} b ON b.id = s.box_id AND b.dw_country_code = s.dw_country_code
LEFT JOIN sales.box_sales bs ON bs.dw_country_code = o.dw_country_code AND bs.user_id = o.user_id AND bs.box_id = s.box_id
WHERE o.status_id IN (1,3) 
AND s.shipping_status_id = 12 
AND bs.user_id IS NULL
GROUP BY ALL
),
user_boxes AS (
  SELECT
    dw_country_code,
    user_id,
    box_id,
    date,
    ROW_NUMBER() OVER (PARTITION BY dw_country_code, user_id ORDER BY date) AS rn
  FROM box_paused
),

with_lag AS (
  SELECT
    *,
    LAG(box_id) OVER (PARTITION BY dw_country_code, user_id ORDER BY rn) AS prev_box_id
  FROM user_boxes
),

grouped_sequences AS (
  SELECT
    *,
    -- Démarre un nouveau groupe quand la box_id n'est pas consécutive à la précédente
    SUM(CASE WHEN prev_box_id IS NULL OR box_id != prev_box_id + 1 THEN 1 ELSE 0 END)
      OVER (PARTITION BY dw_country_code, user_id ORDER BY rn) AS seq_group
  FROM with_lag
),

sequence_start AS (
  SELECT
    dw_country_code,
    user_id,
    box_id,
    date,
    seq_group,
    MIN(box_id) OVER (PARTITION BY dw_country_code, user_id, seq_group) AS start_box_of_sequence,
    MAX(box_id) OVER (PARTITION BY dw_country_code, user_id, seq_group) AS end_box_of_sequence
  FROM grouped_sequences
)
SELECT
  *,
  box_id - start_box_of_sequence AS nb_box_since_pause,
  end_box_of_sequence - box_id AS nb_box_until_end_of_pause
FROM sequence_start