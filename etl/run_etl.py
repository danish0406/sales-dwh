from etl.extract import extract_data
from etl.transform import (
    clean_sales,
    clean_customers,
    clean_products,
)
from etl.dimensions import (
    build_dim_date,
    build_dim_customer,
    build_dim_product,
)
from etl.fact import build_fact_sales
from etl.load import load_table


def run_etl():
    print("🚀 Starting ETL pipeline")

    # ------------------ EXTRACT ------------------
    sales_raw, customers_raw, products_raw = extract_data()

    # ------------------ TRANSFORM ----------------
    sales = clean_sales(sales_raw)
    customers = clean_customers(customers_raw)
    products = clean_products(products_raw)

    # ------------------ DIMENSIONS ----------------
    dim_date = build_dim_date(sales)
    dim_customer = build_dim_customer(customers)
    dim_product = build_dim_product(products)

    # ------------------ LOAD DIMENSIONS -----------
    load_table(dim_date, "dim_date")
    load_table(dim_customer, "dim_customer")
    load_table(dim_product, "dim_product")

    # ------------------ FACT ----------------------
    fact_sales = build_fact_sales(sales, dim_date, dim_customer, dim_product)
    load_table(fact_sales, "fact_sales")

    print("🎉 ETL completed successfully")


if __name__ == "__main__":
    run_etl()
