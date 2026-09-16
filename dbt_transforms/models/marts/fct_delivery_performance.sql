WITH staging AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT
    city,
    COUNT(order_id) AS total_orders,
    ROUND(AVG(prep_time_minutes), 2) AS avg_prep_time_mins,
    ROUND(AVG(transit_time_minutes), 2) AS avg_transit_time_mins,
    ROUND(AVG(total_delivery_minutes), 2) AS avg_total_fulfillment_mins,
    SUM(CASE WHEN is_sla_breached THEN 1 ELSE 0 END) AS sla_breached_count,
    ROUND(
        (SUM(CASE WHEN is_sla_breached THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(order_id), 0)) * 100, 
        2
    ) AS sla_breach_rate_pct
FROM staging
WHERE status = 'SUCCESS'
GROUP BY city
ORDER BY sla_breach_rate_pct DESC