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

