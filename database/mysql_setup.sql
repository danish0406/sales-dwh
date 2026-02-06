-- Create database if not exists
CREATE DATABASE IF NOT EXISTS sales_dwh;
USE sales_dwh;

-- Clear existing tables (for fresh start)
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_salesperson;
DROP TABLE IF EXISTS staging_sales;

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- 1. DATE DIMENSION
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY AUTO_INCREMENT,
    full_date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INT NOT NULL,
    day_of_week VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN DEFAULT FALSE,
    is_holiday BOOLEAN DEFAULT FALSE,
    INDEX idx_full_date (full_date)
);

-- 2. CUSTOMER DIMENSION
CREATE TABLE dim_customer (
    customer_key INT PRIMARY KEY AUTO_INCREMENT,
    customer_id VARCHAR(50) NOT NULL UNIQUE,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    segment VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100) DEFAULT 'USA',
    region VARCHAR(50),
    join_date DATE,
    loyalty_tier VARCHAR(20) DEFAULT 'Standard',
    INDEX idx_customer_id (customer_id),
    INDEX idx_region (region)
);

-- 3. PRODUCT DIMENSION
CREATE TABLE dim_product (
    product_key INT PRIMARY KEY AUTO_INCREMENT,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(200),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    brand VARCHAR(100),
    cost_price DECIMAL(10,2),
    selling_price DECIMAL(10,2),
    supplier VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    INDEX idx_product_id (product_id),
    INDEX idx_category (category)
);

-- 4. SALESPERSON DIMENSION
CREATE TABLE dim_salesperson (
    salesperson_key INT PRIMARY KEY AUTO_INCREMENT,
    salesperson_id VARCHAR(50) NOT NULL UNIQUE,
    salesperson_name VARCHAR(100),
    email VARCHAR(100),
    region VARCHAR(50),
    territory VARCHAR(50),
    manager_id VARCHAR(50),
    hire_date DATE,
    commission_rate DECIMAL(5,2) DEFAULT 0.10,
    INDEX idx_salesperson_id (salesperson_id)
);

-- ============================================
-- STAGING TABLE (for raw data)
-- ============================================

CREATE TABLE staging_sales (
    staging_id INT PRIMARY KEY AUTO_INCREMENT,
    sale_id VARCHAR(50),
    sale_date DATE,
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    category VARCHAR(100),
    customer_id VARCHAR(50),
    customer_name VARCHAR(100),
    quantity INT,
    unit_price DECIMAL(10,2),
    discount DECIMAL(10,2) DEFAULT 0,
    total_amount DECIMAL(10,2),
    payment_method VARCHAR(50),
    shipping_mode VARCHAR(50),
    region VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_sale_date (sale_date),
    INDEX idx_customer_id (customer_id),
    INDEX idx_product_id (product_id)
);

-- ============================================
-- FACT TABLE (core sales data)
-- ============================================

CREATE TABLE fact_sales (
    sales_key BIGINT PRIMARY KEY AUTO_INCREMENT,
    date_key INT NOT NULL,
    product_key INT NOT NULL,
    customer_key INT NOT NULL,
    salesperson_key INT,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount DECIMAL(10,2) DEFAULT 0,
    total_amount DECIMAL(10,2) NOT NULL,
    profit DECIMAL(10,2),
    payment_method VARCHAR(50),
    shipping_mode VARCHAR(50),
    
    -- Foreign keys
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (salesperson_key) REFERENCES dim_salesperson(salesperson_key),
    
    -- Indexes for performance
    INDEX idx_date_key (date_key),
    INDEX idx_product_key (product_key),
    INDEX idx_customer_key (customer_key),
    INDEX idx_salesperson_key (salesperson_key),
    INDEX idx_total_amount (total_amount)
);

-- ============================================
-- SAMPLE DATA INSERTION
-- ============================================

-- Insert sample salespersons
INSERT INTO dim_salesperson (salesperson_id, salesperson_name, region) VALUES
('SP001', 'John Smith', 'North'),
('SP002', 'Sarah Johnson', 'South'),
('SP003', 'Mike Brown', 'East'),
('SP004', 'Lisa Davis', 'West');

-- Show created tables
SELECT 'Database created successfully!' as message;
SHOW TABLES;

-- Count tables
SELECT 
    COUNT(*) as total_tables,
    (SELECT COUNT(*) FROM dim_date) as dim_date_rows,
    (SELECT COUNT(*) FROM dim_customer) as dim_customer_rows,
    (SELECT COUNT(*) FROM dim_product) as dim_product_rows,
    (SELECT COUNT(*) FROM dim_salesperson) as dim_salesperson_rows,
    (SELECT COUNT(*) FROM fact_sales) as fact_sales_rows,
    (SELECT COUNT(*) FROM staging_sales) as staging_sales_rows
FROM information_schema.tables 
WHERE table_schema = 'sales_dwh';