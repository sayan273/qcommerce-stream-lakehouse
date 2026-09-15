CREATE TABLE IF NOT EXISTS raw_users (
    user_id VARCHAR(50) PRIMARY KEY,
    membership_tier VARCHAR(50),
    preferred_city VARCHAR(100),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Initial Mock Data
INSERT INTO raw_users (user_id, membership_tier, preferred_city, updated_at)
VALUES 
    ('USR_1001', 'Silver', 'Bengaluru', NOW() - INTERVAL '2 DAYS'),
    ('USR_1002', 'Gold', 'Mumbai', NOW() - INTERVAL '1 DAY'),
    ('USR_1003', 'Bronze', 'Delhi-NCR', NOW())
ON CONFLICT (user_id) DO UPDATE 
SET membership_tier = EXCLUDED.membership_tier,
    preferred_city = EXCLUDED.preferred_city,
    updated_at = EXCLUDED.updated_at;