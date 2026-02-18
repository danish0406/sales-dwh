import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_sales_data():
    np.random.seed(42)

    dates = pd.date_range(start="2025-01-01", periods=365)
    customers = range(1, 101)
    products = range(1, 11)

    rows = []
    for _ in range(1000):
        date = np.random.choice(dates)
        cust = np.random.choice(customers)
        prod = np.random.choice(products)

        qty = np.random.randint(1, 5)
        price = np.random.randint(100, 1000)
        gross = qty * price
        discount = round(gross * np.random.choice([0, 0.05, 0.1]), 2)

        rows.append([
            date, cust, prod, qty, gross, discount
        ])

    df = pd.DataFrame(rows, columns=[
        "order_date", "customer_id", "product_id",
        "quantity", "gross_sales", "discount"
    ])

    df.to_csv("data/raw/sales.csv", index=False)
    return df
