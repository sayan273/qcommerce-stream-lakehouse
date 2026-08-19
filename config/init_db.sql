CREATE TABLE IF NOT EXISTS raw_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    city VARCHAR(100),
    category VARCHAR(100),
    order_value_inr NUMERIC(10, 2),
    surge_multiplier NUMERIC(4, 2),
    event_timestamp TIMESTAMP WITH TIME ZONE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_orders_timestamp ON raw_orders(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_orders_city ON raw_orders(city);