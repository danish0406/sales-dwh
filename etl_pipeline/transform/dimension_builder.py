"""
Build dimension tables for the data warehouse
STRICT, FK-SAFE, SURROGATE-KEY ENFORCED
"""

import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def generate_date_key(date_obj) -> int:
    """Single source of truth for date_key"""
    return int(date_obj.strftime("%Y%m%d"))


class DimensionBuilder:
    """Build dimension tables with enforced surrogate keys"""

    def __init__(self):
        self.dimensions = {}

    # ------------------------------------------------------------------
    # DATE DIMENSION (CORRECT)
    # ------------------------------------------------------------------
    def build_date_dimension(self, sales_df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Building date dimension (strict mode)...")

        if "sale_date" not in sales_df.columns:
            raise ValueError("sales_df must contain 'sale_date' column")

        sales_dates = pd.to_datetime(sales_df["sale_date"], errors="raise").dt.date

        date_range = pd.date_range(
            start=sales_dates.min(),
            end=sales_dates.max(),
            freq="D"
        )

        date_dim = pd.DataFrame({
            "date_key": [generate_date_key(d) for d in date_range.date],
            "full_date": date_range.date,
            "year": date_range.year,
            "quarter": date_range.quarter,
            "month": date_range.month,
            "month_name": date_range.strftime("%B"),
            "day": date_range.day,
            "day_of_week": date_range.strftime("%A"),
            "day_of_week_num": date_range.dayofweek + 1,
            "week_of_year": date_range.isocalendar().week.astype(int),
            "is_weekend": date_range.dayofweek.isin([5, 6]),
            "is_holiday": False,
        })

        self.dimensions["dim_date"] = date_dim
        logger.info(f"✅ Date dimension created ({len(date_dim)} rows)")
        return date_dim

    # ------------------------------------------------------------------
    # CUSTOMER DIMENSION (FIXED)
    # ------------------------------------------------------------------
    def build_customer_dimension(self, customer_df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Building customer dimension (with surrogate key)...")

        if "customer_id" not in customer_df.columns:
            raise ValueError("customer_df must contain customer_id")

        customer_dim = customer_df.copy()
        customer_dim["customer_id"] = customer_dim["customer_id"].astype(str)

        defaults = {
            "segment": "Consumer",
            "region": "Unknown",
            "city": "Unknown",
            "state": "Unknown",
            "country": "Unknown",
            "loyalty_tier": "Standard",
        }

        for col, default in defaults.items():
            if col not in customer_dim.columns:
                customer_dim[col] = default

        customer_dim = (
            customer_dim
            .drop_duplicates(subset=["customer_id"])
            .reset_index(drop=True)
        )

        # 🔑 SURROGATE KEY (MANDATORY)
        customer_dim.insert(0, "customer_key", range(1, len(customer_dim) + 1))

        self.dimensions["dim_customer"] = customer_dim
        logger.info(f"✅ Customer dimension created ({len(customer_dim)} rows)")
        return customer_dim

    # ------------------------------------------------------------------
    # PRODUCT DIMENSION (FIXED — YOUR BIGGEST BUG)
    # ------------------------------------------------------------------
    def build_product_dimension(self, product_df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Building product dimension (with surrogate key)...")

        if "product_id" not in product_df.columns:
            raise ValueError("product_df must contain product_id")

        product_dim = product_df.copy()
        product_dim["product_id"] = product_dim["product_id"].astype(str)

        defaults = {
            "product_name": "Unknown Product",
            "category": "General",
            "sub_category": "General",
            "brand": "Generic",
            "supplier": "Unknown",
            "cost_price": 0.0,
            "selling_price": 0.0,
            "is_active": True,
        }

        for col, default in defaults.items():
            if col not in product_dim.columns:
                product_dim[col] = default

        product_dim = (
            product_dim
            .drop_duplicates(subset=["product_id"])
            .reset_index(drop=True)
        )

        # 🔑 SURROGATE KEY (THE MISSING PIECE)
        product_dim.insert(0, "product_key", range(1, len(product_dim) + 1))

        # Safe profit margin
        product_dim["profit_margin"] = 0.0
        mask = product_dim["selling_price"] > 0
        product_dim.loc[mask, "profit_margin"] = (
            (product_dim.loc[mask, "selling_price"]
             - product_dim.loc[mask, "cost_price"])
            / product_dim.loc[mask, "selling_price"]
            * 100
        ).round(2)

        self.dimensions["dim_product"] = product_dim
        logger.info(f"✅ Product dimension created ({len(product_dim)} rows)")
        return product_dim

    # ------------------------------------------------------------------
    # SALESPERSON DIMENSION (SAFE)
    # ------------------------------------------------------------------
    def get_existing_salespersons(self, db_connection) -> pd.DataFrame:
        logger.info("Fetching salespersons from database...")

        try:
            cursor = db_connection.cursor(dictionary=True)
            cursor.execute("""
                SELECT salesperson_key,
                       salesperson_id,
                       salesperson_name,
                       region,
                       territory,
                       email,
                       hire_date,
                       commission_rate
                FROM dim_salesperson
            """)
            rows = cursor.fetchall()
            cursor.close()

            if not rows:
                raise ValueError("dim_salesperson empty")

            df = pd.DataFrame(rows)
            self.dimensions["dim_salesperson"] = df
            logger.info(f"✅ Loaded {len(df)} salespersons from DB")
            return df

        except Exception as e:
            logger.warning(f"Salesperson fetch failed ({e}), using defaults")
            return self._create_default_salespersons()

    def _create_default_salespersons(self) -> pd.DataFrame:
        logger.info("Creating default salesperson dimension...")

        df = pd.DataFrame([
            {"salesperson_id": "SP001", "salesperson_name": "John Smith",  "region": "North"},
            {"salesperson_id": "SP002", "salesperson_name": "Sarah Johnson", "region": "South"},
            {"salesperson_id": "SP003", "salesperson_name": "Mike Brown",   "region": "East"},
            {"salesperson_id": "SP004", "salesperson_name": "Lisa Davis",   "region": "West"},
        ])

        df.insert(0, "salesperson_key", range(1, len(df) + 1))
        df["territory"] = df["region"]
        df["email"] = df["salesperson_id"].str.lower() + "@company.com"
        df["hire_date"] = pd.Timestamp("2022-01-01")
        df["commission_rate"] = 0.10

        self.dimensions["dim_salesperson"] = df
        logger.info(f"✅ Default salespersons created ({len(df)} rows)")
        return df

    # ------------------------------------------------------------------
    # UTILITIES
    # ------------------------------------------------------------------
    def save_dimensions(self, output_dir="data/processed"):
        import os
        os.makedirs(output_dir, exist_ok=True)

        for name, df in self.dimensions.items():
            path = f"{output_dir}/{name}.csv"
            df.to_csv(path, index=False)
            logger.info(f"Saved {name} → {path}")

    def get_all_dimensions(self):
        return self.dimensions
