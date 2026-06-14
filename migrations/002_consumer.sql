CREATE TABLE IF NOT EXISTS notification_log (
    event_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    customer_id TEXT NOT NULL,
    status TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS processed_events (
    consumer_name TEXT NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, event_id)
);

CREATE TABLE IF NOT EXISTS order_analytics (
    date DATE PRIMARY KEY,
    order_count INT NOT NULL,
    revenue_cents BIGINT NOT NULL
);
