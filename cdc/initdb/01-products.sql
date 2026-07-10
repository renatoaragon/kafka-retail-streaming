-- Operational (OLTP) table whose row changes flow to Kafka via Debezium.
-- Same retail domain as the event stream: the producer emits sale/stock
-- *events*; this table is the *state* those events act upon.

CREATE TABLE products (
    sku        text PRIMARY KEY,
    category   text NOT NULL,
    unit_price numeric(10, 2) NOT NULL CHECK (unit_price >= 0),
    stock      integer NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- With the default REPLICA IDENTITY (primary key only), an UPDATE/DELETE change
-- event carries just the key of the old row. FULL includes the complete before
-- image, so consumers can see what a value changed *from*, not only *to*.
ALTER TABLE products REPLICA IDENTITY FULL;

-- Synthetic seed rows (same categories/warehouses domain as the generator).
INSERT INTO products (sku, category, unit_price, stock) VALUES
    ('SKU0001', 'books',  12.50, 120),
    ('SKU0002', 'books',  29.90,  40),
    ('SKU0003', 'home',   54.00,  15),
    ('SKU0004', 'home',    8.75, 300),
    ('SKU0005', 'toys',   19.99,  85),
    ('SKU0006', 'toys',   45.50,  22),
    ('SKU0007', 'beauty',  9.90, 150),
    ('SKU0008', 'beauty', 32.00,  60),
    ('SKU0009', 'sports', 74.25,  10),
    ('SKU0010', 'sports', 15.80, 200);
