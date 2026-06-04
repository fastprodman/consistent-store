CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE inventory (
    sku TEXT PRIMARY KEY,
    available_quantity INT NOT NULL CHECK (available_quantity >= 0),
    price_cents INT NOT NULL CHECK (price_cents >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE customers (
    id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    id UUID PRIMARY KEY,
    customer_id TEXT NOT NULL REFERENCES customers(id),
    status TEXT NOT NULL,
    total_cents INT NOT NULL CHECK (total_cents >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(id),
    sku TEXT NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    price_cents INT NOT NULL CHECK (price_cents >= 0)
);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSON NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO inventory (sku, available_quantity, price_cents)
VALUES 
    ('BOOK-001', 10, 1500),
    ('LAPTOP-001', 3, 120000);

INSERT INTO customers (id)
VALUES
    ('customer-001');
