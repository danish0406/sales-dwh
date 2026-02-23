import pandas as pd
from config.db_config import get_connection

def load_customers():
    conn = get_connection()
    cursor = conn.cursor()

    df = pd.read_csv("data/customers_raw.csv")

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO dim_customer (customer_id, age, gender)
            VALUES (%s, %s, %s)
        """, (int(row["customer_id"]), int(row["age"]), row["gender"]))

    conn.commit()
    cursor.close()
    conn.close()

    print("Customers loaded successfully.")

def load_products():
    conn = get_connection()
    cursor = conn.cursor()

    df = pd.read_csv("data/products_raw.csv")

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO dim_product 
            (product_id, product_name, category, cost_price, selling_price)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            int(row["product_id"]),
            row["product_name"],
            row["category"],
            float(row["cost_price"]),
            float(row["selling_price"])
        ))

    conn.commit()
    cursor.close()
    conn.close()

    print("Products loaded successfully.")
  

def load_cities():
    conn = get_connection()
    cursor = conn.cursor()

    cities = [
        ("Mumbai", "West"),
        ("Delhi", "North"),
        ("Bangalore", "South"),
        ("Hyderabad", "South"),
        ("Lucknow", "North")
    ]

    for city_name, region in cities:
        cursor.execute("""
            INSERT INTO dim_city (city_name, region)
            VALUES (%s, %s)
        """, (city_name, region))

    conn.commit()
    cursor.close()
    conn.close()

    print("Cities loaded successfully.")
