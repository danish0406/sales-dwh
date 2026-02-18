from extract import generate_sales_data
from transform import build_dim_date, build_dim_product
from load import load_table

sales = generate_sales_data()

dim_date = build_dim_date(sales)
load_table(dim_date, "dim_date")

dim_product = build_dim_product()
load_table(dim_product, "dim_product")
