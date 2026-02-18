from extract import generate_sales_data
from transform import build_dim_date, build_dim_product
from load import load_table

sales = generate_sales_data()


dim_date = build_dim_date(sales)
load_table(dim_date, "dim_date")

dim_product = build_dim_product()
load_table(dim_product, "dim_product")


from extract import generate_sales_data
from transform import (
    build_dim_date,
    build_dim_product,
    build_dim_customer,
    build_fact_sales
)
from load import load_table

# STEP 1: Extract
sales = generate_sales_data()

# STEP 2: Dimensions
dim_date = build_dim_date(sales)
load_table(dim_date, "dim_date")

dim_product = build_dim_product()
load_table(dim_product, "dim_product")

dim_customer = build_dim_customer(sales)
load_table(dim_customer, "dim_customer")

# STEP 3: Fact
fact_sales = build_fact_sales(
    sales, dim_date, dim_customer, dim_product
)
load_table(fact_sales, "fact_sales")
