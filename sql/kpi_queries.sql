-- Total Revenue
SELECT SUM(revenue) AS total_revenue
FROM fact_sales;


-- Total Orders
SELECT COUNT(*) AS total_orders
FROM fact_sales;


-- Total Orders
SELECT COUNT(*) AS total_orders
FROM fact_sales;


-- Revenue by Year
SELECT 
    d.year,
    SUM(f.revenue) AS total_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year
ORDER BY d.year;


-- Revenue by City
SELECT 
    c.city_name,
    SUM(f.revenue) AS total_revenue
FROM fact_sales f
JOIN dim_city c ON f.city_key = c.city_key
GROUP BY c.city_name
ORDER BY total_revenue DESC;


-- Top 5 Customers
SELECT 
    dc.customer_id,
    SUM(f.revenue) AS total_spent
FROM fact_sales f
JOIN dim_customer dc ON f.customer_key = dc.customer_key
GROUP BY dc.customer_id
ORDER BY total_spent DESC
LIMIT 5;


-- Revenue by Product
SELECT 
    dp.product_name,
    SUM(f.revenue) AS total_revenue
FROM fact_sales f
JOIN dim_product dp ON f.product_key = dp.product_key
GROUP BY dp.product_name
ORDER BY total_revenue DESC;


-- Monthly Revenue Trend
SELECT 
    d.year,
    d.month,
    SUM(f.revenue) AS total_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;