CREATE TABLE IF NOT EXISTS raw_orders (
    order_id VARCHAR(100) PRIMARY KEY,
    user_id VARCHAR(50),
    city VARCHAR(100),
    category VARCHAR(100),
    amount NUMERIC(10, 2),
    payment_mode VARCHAR(50),
    status VARCHAR(20),
    event_timestamp TIMESTAMP WITH TIME ZONE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_orders_timestamp ON raw_orders(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_orders_status ON raw_orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_city ON raw_orders(city);