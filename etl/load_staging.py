import pandas as pd
import mysql.connector

# ===== DB CONNECTION =====
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="danish@sql12345",
    database="retail_sdw"
)

cursor = conn.cursor()

# ===== LOAD CSV FILES =====
customers_df = pd.read_csv("data/customers_raw.csv")
products_df = pd.read_csv("data/products_raw.csv")
sales_df = pd.read_csv("data/sales_raw.csv")

# ===== INSERT CUSTOMERS =====
for _, row in customers_df.iterrows():
    cursor.execute("""
        INSERT INTO staging_customers (customer_id, customer_name, city)
        VALUES (%s, %s, %s)
    """, (row['customer_id'], row['customer_name'], row['city']))

# ===== INSERT PRODUCTS =====
for _, row in products_df.iterrows():
    cursor.execute("""
        INSERT INTO staging_products (product_id, product_name, category, price)
        VALUES (%s, %s, %s, %s)
    """, (row['product_id'], row['product_name'], row['category'], row['price']))

# ===== INSERT SALES =====
for _, row in sales_df.iterrows():
    cursor.execute("""
        INSERT INTO staging_sales (order_id, customer_id, product_id, order_date, quantity, amount)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        row['order_id'],
        row['customer_id'],
        row['product_id'],
        row['order_date'],
        row['quantity'],
        row['amount']
    ))

conn.commit()
cursor.close()
conn.close()

print("Staging data loaded successfully.")
