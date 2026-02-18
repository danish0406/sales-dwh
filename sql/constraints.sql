USE sales_dwh;

ALTER TABLE fact_sales
ADD CONSTRAINT fk_date
FOREIGN KEY (date_key) REFERENCES dim_date(date_key);

ALTER TABLE fact_sales
ADD CONSTRAINT fk_customer
FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key);

ALTER TABLE fact_sales
ADD CONSTRAINT fk_product
FOREIGN KEY (product_key) REFERENCES dim_product(product_key);
