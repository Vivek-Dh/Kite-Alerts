-- Enable the uuid-ossp extension to generate UUIDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION trigger_set_timestamp()
    RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = timezone('utc', now());
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Main table for storing alert definitions
CREATE TABLE IF NOT EXISTS alerts (
                        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                        alert_id VARCHAR(255) NOT NULL,
                        stock_symbol VARCHAR(255) NOT NULL,
                        user_id VARCHAR(255) NOT NULL,
                        price_threshold NUMERIC(10, 2) NOT NULL,
                        condition_type VARCHAR(10) NOT NULL,
                        is_active BOOLEAN NOT NULL DEFAULT TRUE,
                        created_at TIMESTAMP NOT NULL DEFAULT timezone('utc', now()),
                        updated_at TIMESTAMP NOT NULL DEFAULT timezone('utc', now())
);

-- Trigger to automatically update the updated_at timestamp on row update
CREATE TRIGGER set_timestamp
    BEFORE UPDATE ON alerts
    FOR EACH ROW
EXECUTE FUNCTION trigger_set_timestamp();

-- Transactional outbox table for alert change events
CREATE TABLE IF NOT EXISTS alert_outbox_events (
                                     id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                                     payload JSONB NOT NULL,
                                     processed_at TIMESTAMP NULL, -- Changed from processed BOOLEAN to a nullable timestamp
                                     created_at TIMESTAMP NOT NULL DEFAULT timezone('utc', now())
);

-- Indexes to speed up queries
-- A user can only have one unique alert for a given symbol, condition, and price.
CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_user ON alerts (alert_id, user_id);
CREATE INDEX IF NOT EXISTS idx_alerts_stock_symbol ON alerts (stock_symbol);
CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed ON alert_outbox_events (processed_at, created_at) WHERE processed_at IS NULL;

