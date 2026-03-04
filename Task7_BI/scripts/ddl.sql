-- для SQL Server, в Postgres просто игнорируй или используй \c

CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

/* STAGE LAYER */
DROP TABLE IF EXISTS stage.superstore_raw;
CREATE TABLE stage.superstore_raw (
    row_id              INT,
    order_id            VARCHAR(50),
    order_date          DATE,
    ship_date           DATE,
    ship_mode           VARCHAR(50),
    customer_id         VARCHAR(50),
    customer_name       VARCHAR(100),
    segment             VARCHAR(50),
    country             VARCHAR(50),
    city                VARCHAR(50),
    state               VARCHAR(50),
    postal_code         VARCHAR(20),
    region              VARCHAR(50),
    product_id          VARCHAR(50),
    category            VARCHAR(50),
    subcategory         VARCHAR(50),
    product_name        VARCHAR(255),
    sales               NUMERIC(12,2),
    quantity            INT,
    discount            NUMERIC(5,2),
    profit              NUMERIC(12,2),
    load_dttm           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

/* CORE LAYER (normalized star + SCD) */
DROP TABLE IF EXISTS core.dim_date CASCADE;
CREATE TABLE core.dim_date (
    date_key        INT PRIMARY KEY,
    full_date       DATE NOT NULL UNIQUE,
    year            INT,
    quarter         INT,
    month           INT,
    month_name      VARCHAR(20),
    day             INT
);

DROP TABLE IF EXISTS core.dim_customer CASCADE;
CREATE TABLE core.dim_customer (
    customer_key    BIGSERIAL PRIMARY KEY,
    customer_id     VARCHAR(50) NOT NULL,
    customer_name   VARCHAR(100),
    segment         VARCHAR(50),
    country         VARCHAR(50),
    city            VARCHAR(50),
    state           VARCHAR(50),
    postal_code     VARCHAR(20),
    region          VARCHAR(50),
    valid_from      DATE NOT NULL,
    valid_to        DATE,
    is_current      BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT uq_customer_version UNIQUE (customer_id, valid_from)
);
CREATE INDEX idx_customer_business ON core.dim_customer(customer_id);

DROP TABLE IF EXISTS core.dim_product CASCADE;
CREATE TABLE core.dim_product (
    product_key     BIGSERIAL PRIMARY KEY,
    product_id      VARCHAR(50) NOT NULL UNIQUE,
    category        VARCHAR(50),
    subcategory     VARCHAR(50),
    product_name    VARCHAR(255)
);

DROP TABLE IF EXISTS core.dim_ship_mode CASCADE;
CREATE TABLE core.dim_ship_mode (
    ship_mode_key   BIGSERIAL PRIMARY KEY,
    ship_mode       VARCHAR(50) UNIQUE
);

CREATE TABLE IF NOT EXISTS core.fact_sales (
    sales_key       BIGSERIAL PRIMARY KEY,
    order_id        VARCHAR(50),
    order_date_key  INT NOT NULL,
    ship_date_key   INT NOT NULL,
    customer_key    BIGINT NOT NULL,
    product_key     BIGINT NOT NULL,
    ship_mode_key   BIGINT NOT NULL,
    sales           NUMERIC(12,2),
    quantity        INT,
    discount        NUMERIC(5,2),
    profit          NUMERIC(12,2),

    CONSTRAINT uq_order_product UNIQUE (order_id, product_key),

    CONSTRAINT fk_order_date  FOREIGN KEY (order_date_key) REFERENCES core.dim_date(date_key),
    CONSTRAINT fk_ship_date   FOREIGN KEY (ship_date_key) REFERENCES core.dim_date(date_key),
    CONSTRAINT fk_customer    FOREIGN KEY (customer_key)  REFERENCES core.dim_customer(customer_key),
    CONSTRAINT fk_product     FOREIGN KEY (product_key)   REFERENCES core.dim_product(product_key),
    CONSTRAINT fk_ship_mode   FOREIGN KEY (ship_mode_key) REFERENCES core.dim_ship_mode(ship_mode_key)
);

/* MART LAYER (denormalized flat для Power BI) */
DROP TABLE IF EXISTS mart.dim_date CASCADE;
CREATE TABLE mart.dim_date AS SELECT * FROM core.dim_date;
ALTER TABLE mart.dim_date ADD PRIMARY KEY (date_key);

DROP TABLE IF EXISTS mart.dim_customer CASCADE;
CREATE TABLE mart.dim_customer AS SELECT * FROM core.dim_customer;
ALTER TABLE mart.dim_customer ADD PRIMARY KEY (customer_key);

DROP TABLE IF EXISTS mart.dim_product CASCADE;
CREATE TABLE mart.dim_product AS SELECT * FROM core.dim_product;
ALTER TABLE mart.dim_product ADD PRIMARY KEY (product_key);

DROP TABLE IF EXISTS mart.fact_sales CASCADE;
CREATE TABLE mart.fact_sales (
    sales_key       BIGSERIAL PRIMARY KEY,
    order_id        VARCHAR(50),
    order_date      DATE,
    customer_name   VARCHAR(100),
    segment         VARCHAR(50),
    product_name    VARCHAR(255),
    sales           NUMERIC(12,2),
    quantity        INT,
    profit          NUMERIC(12,2)
);