WITH staging AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

hourly_metrics AS (
    SELECT
        DATE_TRUNC('hour', order_timestamp) AS metric_hour,
        city,
        category,
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(order_value_inr), 2) AS total_gmv_inr,
        ROUND(AVG(order_value_inr), 2) AS avg_order_value_inr,
        ROUND(AVG(surge_multiplier), 2) AS avg_surge_multiplier,
        SUM(CASE WHEN surge_multiplier > 1.2 THEN 1 ELSE 0 END) AS high_surge_order_count
    FROM staging
    GROUP BY 1, 2, 3
)

SELECT * FROM hourly_metrics
ORDER BY metric_hour DESC, total_gmv_inr DESC