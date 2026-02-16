"""
Create fact table by joining sales data with dimension surrogate keys
STRICT FK-SAFE VERSION
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def generate_date_key(date_obj) -> int:
    """Single source of truth for date_key"""
    return int(date_obj.strftime("%Y%m%d"))


class FactTableCreator:
    """Create fact table with strict FK enforcement"""

    def __init__(self):
        self.fact_table = None

    def create_fact_table(self, sales_df, dimensions, db_connection=None):
        logger.info("Creating fact table (strict mode)...")

        fact_df = sales_df.copy()

        # ------------------------------------------------------------------
        # DATE KEY (MANDATORY)
        # ------------------------------------------------------------------
        fact_df["sale_date_only"] = pd.to_datetime(
            fact_df["sale_date"], errors="raise"
        ).dt.date

        if "dim_date" not in dimensions:
            raise ValueError("dim_date is required to build fact table")

        date_dim = dimensions["dim_date"][["full_date", "date_key"]].copy()
        date_dim["full_date"] = pd.to_datetime(date_dim["full_date"]).dt.date

        fact_df = fact_df.merge(
            date_dim,
            left_on="sale_date_only",
            right_on="full_date",
            how="left",
        )

        if fact_df["date_key"].isna().any():
            missing = fact_df[fact_df["date_key"].isna()]["sale_date_only"].unique()
            raise ValueError(
                f"Fact table has {len(missing)} dates not present in dim_date: {missing[:5]}"
            )

        # ------------------------------------------------------------------
        # PRODUCT KEY (MANDATORY)
        # ------------------------------------------------------------------
        if "dim_product" not in dimensions:
            raise ValueError("dim_product is required to build fact table")

        product_dim = dimensions["dim_product"][["product_id", "product_key"]].copy()
        product_dim["product_id"] = product_dim["product_id"].astype(str)

        fact_df["product_id"] = fact_df["product_id"].astype(str)

        fact_df = fact_df.merge(product_dim, on="product_id", how="left")

        if fact_df["product_key"].isna().any():
            missing = fact_df[fact_df["product_key"].isna()]["product_id"].unique()
            raise ValueError(
                f"Missing product_key for products: {missing[:5]}"
            )

        # ------------------------------------------------------------------
        # CUSTOMER KEY (MANDATORY)
        # ------------------------------------------------------------------
        if "dim_customer" not in dimensions:
            raise ValueError("dim_customer is required to build fact table")

        customer_dim = dimensions["dim_customer"][["customer_id", "customer_key"]].copy()
        customer_dim["customer_id"] = customer_dim["customer_id"].astype(str)

        fact_df["customer_id"] = fact_df["customer_id"].astype(str)

        fact_df = fact_df.merge(customer_dim, on="customer_id", how="left")

        if fact_df["customer_key"].isna().any():
            missing = fact_df[fact_df["customer_key"].isna()]["customer_id"].unique()
            raise ValueError(
                f"Missing customer_key for customers: {missing[:5]}"
            )

        # ------------------------------------------------------------------
        # SALESPERSON KEY (MANDATORY)
        # ------------------------------------------------------------------
        if "dim_salesperson" not in dimensions:
            raise ValueError("dim_salesperson is required to build fact table")

        salesperson_dim = dimensions["dim_salesperson"][
            ["salesperson_key", "region"]
        ].copy()

        region_map = (
            salesperson_dim
            .drop_duplicates("region")
            .set_index("region")["salesperson_key"]
            .to_dict()
        )

        fact_df["salesperson_key"] = fact_df["region"].map(region_map)

        if fact_df["salesperson_key"].isna().any():
            missing = fact_df[fact_df["salesperson_key"].isna()]["region"].unique()
            raise ValueError(
                f"No salesperson_key mapped for regions: {missing}"
            )

        # ------------------------------------------------------------------
        # PROFIT CALCULATION
        # ------------------------------------------------------------------
        if "cost_price" in dimensions["dim_product"].columns:
            cost_map = dimensions["dim_product"].set_index("product_id")["cost_price"]
            fact_df["cost_price"] = fact_df["product_id"].map(cost_map)
            fact_df["profit"] = (
                fact_df["total_amount"]
                - fact_df["cost_price"] * fact_df["quantity"]
            )
        else:
            fact_df["profit"] = fact_df["total_amount"] * 0.2

        # ------------------------------------------------------------------
        # FINAL FACT TABLE
        # ------------------------------------------------------------------
        final_columns = [
            "date_key",
            "product_key",
            "customer_key",
            "salesperson_key",
            "quantity",
            "unit_price",
            "discount",
            "total_amount",
            "profit",
        ]

        if "payment_method" in fact_df.columns:
            final_columns.append("payment_method")
        else:
            fact_df["payment_method"] = "Credit Card"
            final_columns.append("payment_method")

        if "shipping_mode" in fact_df.columns:
            final_columns.append("shipping_mode")
        else:
            fact_df["shipping_mode"] = "Standard"
            final_columns.append("shipping_mode")

        final_fact_df = fact_df[final_columns].copy()

        # Enforce integer FK types
        for col in ["date_key", "product_key", "customer_key", "salesperson_key"]:
            final_fact_df[col] = final_fact_df[col].astype(int)

        self.fact_table = final_fact_df

        logger.info(f"✅ Fact table created: {len(final_fact_df)} rows")
        logger.info(f"Total revenue: ${final_fact_df['total_amount'].sum():,.2f}")
        logger.info(f"Total profit: ${final_fact_df['profit'].sum():,.2f}")

        return final_fact_df

    def save_fact_table(self, filename="data/processed/fact_sales.csv"):
        if self.fact_table is None:
            raise RuntimeError("Fact table not created")

        import os
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        self.fact_table.to_csv(filename, index=False)
        logger.info(f"Saved fact table → {filename}")
