"""
Project: SQL – Sales & Revenue Analytics for an E-commerce Business
Focus: SQL layer and views for BI (Power BI / Tableau / Metabase)
Source: Portfolio spec by Rodrigo Blasi Olandoski
"""

from sqlalchemy import create_engine, text

# 1. DATABASE CONNECTION ---------------------------------------------------

# TODO: replace with your real credentials and database name
DB_URL = "postgresql+psycopg2://user:password@localhost:5432/ecommerce_db"
engine = create_engine(DB_URL, future=True)


# 2. CORE TABLES -----------------------------------------------------------

DDL_CORE_TABLES = """
CREATE SCHEMA IF NOT EXISTS analytics_ecommerce;

CREATE TABLE IF NOT EXISTS analytics_ecommerce.customers (
    customer_id       SERIAL PRIMARY KEY,
    full_name         VARCHAR(255) NOT NULL,
    registration_date DATE NOT NULL,
    city              VARCHAR(120),
    state             VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS analytics_ecommerce.products (
    product_id    SERIAL PRIMARY KEY,
    product_name  VARCHAR(255) NOT NULL,
    category      VARCHAR(120),
    unit_price    NUMERIC(18,2) NOT NULL,
    current_stock NUMERIC(18,2) NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS analytics_ecommerce.orders (
    order_id    SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES analytics_ecommerce.customers(customer_id),
    order_date  TIMESTAMP NOT NULL,
    status      VARCHAR(30) NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics_ecommerce.order_items (
    order_id    INT NOT NULL REFERENCES analytics_ecommerce.orders(order_id),
    product_id  INT NOT NULL REFERENCES analytics_ecommerce.products(product_id),
    quantity    NUMERIC(18,2) NOT NULL,
    unit_price  NUMERIC(18,2) NOT NULL,
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE IF NOT EXISTS analytics_ecommerce.payments (
    payment_id     SERIAL PRIMARY KEY,
    order_id       INT NOT NULL REFERENCES analytics_ecommerce.orders(order_id),
    payment_method VARCHAR(50) NOT NULL,
    amount_paid    NUMERIC(18,2) NOT NULL,
    payment_date   TIMESTAMP NOT NULL
);
"""


# 3. INDEXES ---------------------------------------------------------------

DDL_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_orders_status_date
    ON analytics_ecommerce.orders(order_date, status, customer_id);

CREATE INDEX IF NOT EXISTS idx_order_items_product
    ON analytics_ecommerce.order_items(product_id);

CREATE INDEX IF NOT EXISTS idx_payments_order_method
    ON analytics_ecommerce.payments(order_id, payment_method);
"""


# 4. ANALYTICAL VIEWS ------------------------------------------------------

VIEW_MONTHLY_REVENUE = """
CREATE OR REPLACE VIEW analytics_ecommerce.vw_monthly_revenue AS
SELECT
    DATE_TRUNC('month', o.order_date) AS month,
    SUM(oi.quantity * oi.unit_price)  AS gross_revenue
FROM analytics_ecommerce.orders o
JOIN analytics_ecommerce.order_items oi
  ON oi.order_id = o.order_id
WHERE o.status = 'COMPLETED'
GROUP BY DATE_TRUNC('month', o.order_date)
ORDER BY month;
"""

VIEW_TOP_PRODUCTS = """
CREATE OR REPLACE VIEW analytics_ecommerce.vw_top_products AS
SELECT
    p.product_id,
    p.product_name,
    SUM(oi.quantity * oi.unit_price) AS revenue
FROM analytics_ecommerce.order_items oi
JOIN analytics_ecommerce.products p
  ON p.product_id = oi.product_id
JOIN analytics_ecommerce.orders o
  ON o.order_id = oi.order_id
WHERE o.status = 'COMPLETED'
GROUP BY p.product_id, p.product_name
ORDER BY revenue DESC
LIMIT 10;
"""

VIEW_CUSTOMER_LTV = """
CREATE OR REPLACE VIEW analytics_ecommerce.vw_customer_ltv AS
SELECT
    c.customer_id,
    c.full_name,
    COUNT(DISTINCT o.order_id)       AS total_orders,
    SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM analytics_ecommerce.customers c
JOIN analytics_ecommerce.orders o
  ON o.customer_id = c.customer_id
JOIN analytics_ecommerce.order_items oi
  ON oi.order_id = o.order_id
WHERE o.status = 'COMPLETED'
GROUP BY c.customer_id, c.full_name
ORDER BY total_revenue DESC;
"""

VIEW_STOCK_RISK = """
CREATE OR REPLACE VIEW analytics_ecommerce.vw_stock_risk AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.current_stock,
    COALESCE(SUM(oi.quantity), 0) AS qty_sold_last_30d
FROM analytics_ecommerce.products p
LEFT JOIN analytics_ecommerce.order_items oi
  ON oi.product_id = p.product_id
LEFT JOIN analytics_ecommerce.orders o
  ON o.order_id = oi.order_id
 AND o.order_date >= CURRENT_DATE - INTERVAL '30 days'
 AND o.status = 'COMPLETED'
GROUP BY p.product_id, p.product_name, p.category, p.current_stock
HAVING p.current_stock < COALESCE(SUM(oi.quantity), 0)
ORDER BY p.current_stock ASC;
"""


def bootstrap_schema():
    """Create schema, tables and indexes for the e-commerce analytics layer."""
    with engine.begin() as conn:
        conn.execute(text(DDL_CORE_TABLES))
        conn.execute(text(DDL_INDEXES))


def create_views():
    """Create analytical views that BI tools will consume."""
    with engine.begin() as conn:
        conn.execute(text(VIEW_MONTHLY_REVENUE))
        conn.execute(text(VIEW_TOP_PRODUCTS))
        conn.execute(text(VIEW_CUSTOMER_LTV))
        conn.execute(text(VIEW_STOCK_RISK))


if __name__ == "__main__":
    bootstrap_schema()
    create_views()
    print("E-commerce analytics schema and views created successfully.")
