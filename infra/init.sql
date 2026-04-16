CREATE TABLE IF NOT EXISTS products (
    id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price NUMERIC(12,2) NOT NULL,
    stock_qty INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS clickstream_events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(100) NOT NULL UNIQUE,
    user_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(30) NOT NULL CHECK (event_type IN ('view', 'add_to_cart', 'purchase')),
    event_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_clickstream_product
        FOREIGN KEY (product_id) REFERENCES products(id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS product_window_metrics (
    id BIGSERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    views INT NOT NULL DEFAULT 0,
    add_to_cart_count INT NOT NULL DEFAULT 0,
    purchases INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_product_window UNIQUE (product_id, window_start, window_end)
);

CREATE TABLE IF NOT EXISTS flash_sale_alerts (
    id BIGSERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    views INT NOT NULL,
    purchases INT NOT NULL,
    recommendation VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_flash_alert UNIQUE (product_id, window_start, window_end, recommendation)
);

CREATE TABLE IF NOT EXISTS daily_user_segments (
    id BIGSERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    total_views INT NOT NULL DEFAULT 0,
    total_add_to_cart INT NOT NULL DEFAULT 0,
    total_purchases INT NOT NULL DEFAULT 0,
    segment VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_daily_user_segment UNIQUE (report_date, user_id)
);

CREATE TABLE IF NOT EXISTS daily_top_products (
    id BIGSERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    rank_no INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    total_views INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_daily_top_product UNIQUE (report_date, rank_no)
);

CREATE TABLE IF NOT EXISTS daily_conversion_report (
    id BIGSERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    category VARCHAR(100) NOT NULL,
    total_views INT NOT NULL DEFAULT 0,
    total_purchases INT NOT NULL DEFAULT 0,
    conversion_rate NUMERIC(8,4) NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_daily_conversion_report UNIQUE (report_date, category)
);

CREATE INDEX IF NOT EXISTS idx_clickstream_events_event_time
    ON clickstream_events(event_time);

CREATE INDEX IF NOT EXISTS idx_clickstream_events_product_id
    ON clickstream_events(product_id);

CREATE INDEX IF NOT EXISTS idx_clickstream_events_user_id
    ON clickstream_events(user_id);

CREATE INDEX IF NOT EXISTS idx_product_window_metrics_product_id
    ON product_window_metrics(product_id);

CREATE INDEX IF NOT EXISTS idx_product_window_metrics_window
    ON product_window_metrics(window_start, window_end);

CREATE INDEX IF NOT EXISTS idx_flash_sale_alerts_product_id
    ON flash_sale_alerts(product_id);

CREATE INDEX IF NOT EXISTS idx_flash_sale_alerts_created_at
    ON flash_sale_alerts(created_at);

CREATE INDEX IF NOT EXISTS idx_daily_user_segments_report_date
    ON daily_user_segments(report_date);

CREATE INDEX IF NOT EXISTS idx_daily_top_products_report_date
    ON daily_top_products(report_date);

CREATE INDEX IF NOT EXISTS idx_daily_conversion_report_report_date
    ON daily_conversion_report(report_date);

INSERT INTO products (id, product_name, category, price, stock_qty) VALUES
('P1001', 'iPhone 15', 'Mobile Phones', 999.99, 20),
('P1002', 'Samsung Galaxy S24', 'Mobile Phones', 899.99, 18),
('P1003', 'Sony WH-1000XM5', 'Audio', 349.99, 25),
('P1004', 'Apple MacBook Air M3', 'Laptops', 1299.99, 10),
('P1005', 'Dell XPS 13', 'Laptops', 1199.99, 12),
('P1006', 'Logitech MX Master 3S', 'Accessories', 99.99, 40)
ON CONFLICT (id) DO NOTHING;
