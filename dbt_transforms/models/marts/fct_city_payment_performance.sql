WITH staging AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

payment_metrics AS (
    SELECT
        city,
        payment_mode,
        COUNT(order_id) AS total_transactions,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_transactions,
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_transactions,
        SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS pending_transactions,
        ROUND(
            (SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(order_id), 0)) * 100, 
            2
        ) AS success_rate_pct,
        ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END), 2) AS realized_revenue_inr,
        ROUND(SUM(CASE WHEN status = 'FAILED' THEN amount ELSE 0 END), 2) AS lost_revenue_inr
    FROM staging
    GROUP BY city, payment_mode
)

SELECT * FROM payment_metrics
ORDER BY city ASC, total_transactions DESC