import pandas as pd

def build_dim_date(df):
    dim = pd.DataFrame()
    dim["full_date"] = pd.to_datetime(df["order_date"]).dt.date

    dim = dim.drop_duplicates().reset_index(drop=True)

    dim["date_key"] = pd.to_datetime(dim["full_date"]).dt.strftime("%Y%m%d").astype(int)
    dim["day"] = pd.to_datetime(dim["full_date"]).dt.day
    dim["month"] = pd.to_datetime(dim["full_date"]).dt.month
    dim["month_name"] = pd.to_datetime(dim["full_date"]).dt.month_name()
    dim["quarter"] = pd.to_datetime(dim["full_date"]).dt.quarter
    dim["year"] = pd.to_datetime(dim["full_date"]).dt.year
    dim["is_weekend"] = (pd.to_datetime(dim["full_date"]).dt.weekday >= 5).astype(int)

    return dim


def build_dim_product():
    products = []
    for i in range(1, 11):
        products.append([i, f"Product {i}", "General", 1])

    return pd.DataFrame(products, columns=[
        "product_id", "product_name", "category", "is_active"
    ])

def build_dim_customer(df):
    customers = (
        df[["customer_id"]]
        .drop_duplicates()
        .sort_values("customer_id")
        .reset_index(drop=True)
    )

    # Fake but realistic attributes
    customers["customer_name"] = customers["customer_id"].apply(
        lambda x: f"Customer {x}"
    )
    customers["city"] = "Unknown"
    customers["state"] = "Unknown"
    customers["country"] = "India"

    return customers

def build_fact_sales(sales_df, dim_date, dim_customer, dim_product):
    # Date mapping
    date_map = dict(
        zip(dim_date["full_date"], dim_date["date_key"])
    )

    # Customer surrogate keys (assigned by MySQL later, so we map by natural key)
    cust_map = dict(
        zip(dim_customer["customer_id"], range(1, len(dim_customer) + 1))
    )

    prod_map = dict(
        zip(dim_product["product_id"], range(1, len(dim_product) + 1))
    )

    fact = sales_df.copy()

    fact["date_key"] = fact["order_date"].map(date_map)
    fact["customer_key"] = fact["customer_id"].map(cust_map)
    fact["product_key"] = fact["product_id"].map(prod_map)

    fact["net_sales"] = fact["gross_sales"] - fact["discount"]

    return fact[[
        "date_key",
        "customer_key",
        "product_key",
        "quantity",
        "gross_sales",
        "discount",
        "net_sales"
    ]]
