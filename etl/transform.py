import pandas as pd

def build_dim_date(df):
    dates = pd.to_datetime(df["order_date"].unique())
    dim = pd.DataFrame({"full_date": dates})

    dim["date_key"] = dim["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim["day"] = dim["full_date"].dt.day
    dim["month"] = dim["full_date"].dt.month
    dim["month_name"] = dim["full_date"].dt.month_name()
    dim["quarter"] = dim["full_date"].dt.quarter
    dim["year"] = dim["full_date"].dt.year
    dim["is_weekend"] = dim["full_date"].dt.weekday >= 5

    dim["is_weekend"] = dim["is_weekend"].astype(int)

    return dim

def build_dim_product():
    products = []
    for i in range(1, 11):
        products.append([i, f"Product {i}", "General", 1])

    return pd.DataFrame(products, columns=[
        "product_id", "product_name", "category", "is_active"
    ])
