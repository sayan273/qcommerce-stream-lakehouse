WITH source AS (
    SELECT * FROM {{ source('raw_data', 'raw_orders') }}
),

cleaned AS (
    SELECT
        order_id,
        TRIM(LOWER(city)) AS city,
        TRIM(LOWER(category)) AS category,
        CAST(order_value_inr AS NUMERIC(10, 2)) AS order_value_inr,
        CAST(surge_multiplier AS NUMERIC(4, 2)) AS surge_multiplier,
        CAST(event_timestamp AS TIMESTAMP WITH TIME ZONE) AS order_timestamp,
        CAST(ingested_at AS TIMESTAMP WITH TIME ZONE) AS ingested_at
    FROM source
    WHERE order_id IS NOT NULL
)

SELECT * FROM cleaned