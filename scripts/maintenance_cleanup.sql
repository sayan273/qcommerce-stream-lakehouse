-- Archive or clean up Bronze raw events older than 30 days
DELETE FROM raw_orders
WHERE event_timestamp < NOW() - INTERVAL '30 DAYS';

-- Reclaim storage and rebuild statistics
VACUUM ANALYZE raw_orders;
VACUUM ANALYZE analytics.fct_city_payment_performance;
VACUUM ANALYZE analytics.fct_hourly_city_metrics;