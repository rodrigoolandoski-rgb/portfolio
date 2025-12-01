"""
Project: Enterprise Sales & Margin Data Warehouse (Advanced SQL)
Focus: Dimensional modeling, SCD Type 2, and analytical queries
Source: Advanced portfolio spec by Rodrigo Blasi Olandoski
"""

from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://user:password@localhost:5432/dw_sales"
engine = create_engine(DB_URL, future=True)


DDL_DW_TABLES = """
CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_id    INT PRIMARY KEY,
    date       DATE NOT NULL,
    day        INT,
    month      INT,
    month_name VARCHAR(20),
    year       INT
);

CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_sk    BIGSERIAL PRIMARY KEY,
    product_id    VARCHAR(50) NOT NULL,
    product_name  VARCHAR(255) NOT NULL,
    category      VARCHAR(100),
    brand         VARCHAR(100),
    standard_cost NUMERIC(18,4),
    list_price    NUMERIC(18,4),
    is_current    BOOLEAN NOT NULL,
    valid_from    DATE NOT NULL,
    valid_to      DATE
);

CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_sk   BIGSERIAL PRIMARY KEY,
    customer_id   VARCHAR(50) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    segment       VARCHAR(100),
    region        VARCHAR(100),
    is_current    BOOLEAN NOT NULL,
    valid_from    DATE NOT NULL,
    valid_to      DATE
);

CREATE TABLE IF NOT EXISTS dw.dim_store (
    store_sk   BIGSERIAL PRIMARY KEY,
    store_id   VARCHAR(50) NOT NULL,
    store_name VARCHAR(255),
    channel    VARCHAR(50),
    region     VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dw.fact_sales (
    sales_id        BIGSERIAL PRIMARY KEY,
    date_id         INT NOT NULL REFERENCES dw.dim_date(date_id),
    product_sk      BIGINT NOT NULL REFERENCES dw.dim_product(product_sk),
    customer_sk     BIGINT REFERENCES dw.dim_customer(customer_sk),
    store_sk        BIGINT NOT NULL REFERENCES dw.dim_store(store_sk),
    order_id        VARCHAR(50) NOT NULL,
    quantity        NUMERIC(18,2) NOT NULL,
    gross_amount    NUMERIC(18,2) NOT NULL,
    discount_amount NUMERIC(18,2) DEFAULT 0,
    net_amount      NUMERIC(18,2) NOT NULL,
    cost_amount     NUMERIC(18,2) NOT NULL,
    margin_amount   NUMERIC(18,2) NOT NULL,
    margin_percent  NUMERIC(5,2),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


SCD2_DIM_PRODUCT = """
WITH changed AS (
    SELECT
        o.product_id,
        o.product_name,
        o.category,
        o.brand,
        o.standard_cost,
        o.list_price
    FROM ods_products o
    LEFT JOIN dw.dim_product d
           ON d.product_id = o.product_id
          AND d.is_current = TRUE
    WHERE d.product_id IS NULL
       OR (
            d.product_name  IS DISTINCT FROM o.product_name OR
            d.category      IS DISTINCT FROM o.category OR
            d.brand         IS DISTINCT FROM o.brand OR
            d.standard_cost IS DISTINCT FROM o.standard_cost OR
            d.list_price    IS DISTINCT FROM o.list_price
          )
),
to_close AS (
    UPDATE dw.dim_product d
       SET is_current = FALSE,
           valid_to   = CURRENT_DATE - INTERVAL '1 day'
    FROM changed c
    WHERE d.product_id = c.product_id
      AND d.is_current = TRUE
    RETURNING d.product_id
)
INSERT INTO dw.dim_product (
    product_id, product_name, category, brand,
    standard_cost, list_price,
    is_current, valid_from, valid_to
)
SELECT
    c.product_id, c.product_name, c.category, c.brand,
    c.standard_cost, c.list_price,
    TRUE AS is_current,
    CURRENT_DATE AS valid_from,
    NULL AS valid_to
FROM changed c;
"""


QUERY_MARGIN_BY_PRODUCT_MONTH = """
SELECT
    dd.year,
    dd.month,
    dp.category,
    dp.brand,
    dp.product_name,
    SUM(fs.net_amount)     AS net_revenue,
    SUM(fs.margin_amount)  AS margin_value,
    AVG(fs.margin_percent) AS avg_margin_percent
FROM dw.fact_sales fs
JOIN dw.dim_date dd
  ON dd.date_id = fs.date_id
JOIN dw.dim_product dp
  ON dp.product_sk = fs.product_sk
 AND dp.is_current = TRUE
GROUP BY dd.year, dd.month, dp.category, dp.brand, dp.product_name
ORDER BY dd.year, dd.month, dp.category, dp.brand;
"""

QUERY_CHANNEL_MIX = """
SELECT
    ds.channel,
    dd.year,
    dd.month,
    SUM(fs.net_amount)    AS net_revenue,
    SUM(fs.margin_amount) AS margin_value
FROM dw.fact_sales fs
JOIN dw.dim_store ds
  ON ds.store_sk = fs.store_sk
JOIN dw.dim_date dd
  ON dd.date_id = fs.date_id
GROUP BY ds.channel, dd.year, dd.month
ORDER BY dd.year, dd.month, ds.channel;
"""

QUERY_TOP_CUSTOMERS_MARGIN_12M = """
SELECT
    dc.customer_id,
    dc.customer_name,
    dc.segment,
    SUM(fs.margin_amount) AS total_margin
FROM dw.fact_sales fs
JOIN dw.dim_customer dc
  ON dc.customer_sk = fs.customer_sk
 AND dc.is_current = TRUE
JOIN dw.dim_date dd
  ON dd.date_id = fs.date_id
WHERE dd.date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY dc.customer_id, dc.customer_name, dc.segment
ORDER BY total_margin DESC
LIMIT 20;
"""


def create_dw_tables():
    with engine.begin() as conn:
        conn.execute(text(DDL_DW_TABLES))


def run_scd2_product():
    with engine.begin() as conn:
        conn.execute(text(SCD2_DIM_PRODUCT))


if __name__ == "__main__":
    create_dw_tables()
    print("DW tables created. Include run_scd2_product() in your ETL pipeline.")
