CREATE DATABASE IF NOT EXISTS sales_dwh;
USE sales_dwh;

-- ======================
-- DIM DATE
-- ======================
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day INT,
    month INT,
    month_name VARCHAR(15),
    quarter INT,
    year INT,
    is_weekend TINYINT(1)
);

-- ======================
-- DIM CUSTOMER
-- ======================
CREATE TABLE dim_customer (
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    customer_name VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50)
);

-- ======================
-- DIM PRODUCT
-- ======================
CREATE TABLE dim_product (
    product_key INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT,
    product_name VARCHAR(100),
    category VARCHAR(50),
    is_active TINYINT(1)
);

-- ======================
-- FACT SALES
-- ======================
CREATE TABLE fact_sales (
    sales_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT,
    customer_key INT,
    product_key INT,
    quantity INT,
    gross_sales DECIMAL(10,2),
    discount DECIMAL(10,2),
    net_sales DECIMAL(10,2)
);
