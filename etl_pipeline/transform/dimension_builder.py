"""
Build dimension tables for data warehouse
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class DimensionBuilder:
    """Build dimension tables from cleaned data"""

    def __init__(self):
        self.dimensions = {}

    def build_date_dimension(self, sales_df):
        """Create date dimension table from sales data dates"""
        logger.info("Building date dimension...")

        if "sale_date" not in sales_df.columns:
            logger.error("Sales data must have 'sale_date' column")
            return None

        # Convert to datetime
        sales_df["sale_date"] = pd.to_datetime(sales_df["sale_date"])

        # Get min and max dates from sales
        min_date = sales_df["sale_date"].min().date()
        max_date = sales_df["sale_date"].max().date()

        logger.info(f"Sales data date range: {min_date} to {max_date}")

        # Create full date range with padding to ensure all dates are covered
        # Add 30 days padding at both ends to be safe
        start_date = min_date - timedelta(days=30)
        end_date = max_date + timedelta(days=30)

        # Create full date range
        date_range = pd.date_range(start=start_date, end=end_date, freq="D")

        # Create date dimension with YYYYMMDD as key
        date_dim = pd.DataFrame(
            {
                "full_date": date_range.date,
                "date_key": date_range.strftime("%Y%m%d").astype(int),
                "year": date_range.year,
                "quarter": date_range.quarter,
                "month": date_range.month,
                "month_name": date_range.strftime("%B"),
                "day": date_range.day,
                "day_of_week": date_range.strftime("%A"),
                "day_of_week_num": date_range.dayofweek + 1,  # Monday=1, Sunday=7
                "is_weekend": date_range.dayofweek.isin([5, 6]),
                "is_holiday": False,  # Simplified - could add holiday calendar
                "week_of_year": date_range.isocalendar().week,
            }
        )

        # Sort by date
        date_dim = date_dim.sort_values("full_date").reset_index(drop=True)

        logger.info(f"✅ Date dimension created: {len(date_dim)} dates")
        logger.info(
            f"   Range: {date_dim['full_date'].min()} to {date_dim['full_date'].max()}"
        )
        logger.info(
            f"   Date keys: {date_dim['date_key'].min()} to {date_dim['date_key'].max()}"
        )

        # Validate that all sales dates are covered
        sales_dates = set(sales_df["sale_date"].dt.date.unique())
        dim_dates = set(date_dim["full_date"])

        missing_dates = sales_dates - dim_dates
        if missing_dates:
            logger.warning(
                f"⚠️  Missing {len(missing_dates)} dates in dimension: {sorted(missing_dates)[:5]}"
            )
        else:
            logger.info(
                f"✅ All {len(sales_dates)} sales dates are covered in date dimension"
            )

        self.dimensions["dim_date"] = date_dim
        return date_dim

    def build_customer_dimension(self, customer_df):
        """Create customer dimension table"""
        logger.info("Building customer dimension...")

        # Required columns
        required_cols = [
            "customer_id",
            "customer_name",
            "email",
            "segment",
            "city",
            "state",
            "country",
            "region",
        ]

        # Check if we have required columns
        missing_cols = [col for col in required_cols if col not in customer_df.columns]
        if missing_cols:
            logger.warning(f"Missing columns in customer data: {missing_cols}")

        # Create customer dimension
        customer_dim = customer_df.copy()

        # Add missing columns with defaults
        if "segment" not in customer_dim.columns:
            customer_dim["segment"] = "Consumer"

        if "region" not in customer_dim.columns:
            customer_dim["region"] = "Unknown"

        if "loyalty_tier" not in customer_dim.columns:
            customer_dim["loyalty_tier"] = "Standard"

        if "city" not in customer_dim.columns:
            customer_dim["city"] = "Unknown"

        if "state" not in customer_dim.columns:
            customer_dim["state"] = "Unknown"

        if "country" not in customer_dim.columns:
            customer_dim["country"] = "Unknown"

        # Ensure customer_id is string for consistent joining
        customer_dim["customer_id"] = customer_dim["customer_id"].astype(str)

        # Add surrogate key (will be replaced by database auto-increment)
        # But we keep the original ID for mapping
        customer_dim = customer_dim.reset_index(drop=True)

        logger.info(f"✅ Customer dimension created: {len(customer_dim)} customers")
        logger.info(
            f"   Customer IDs: {customer_dim['customer_id'].iloc[0]} to {customer_dim['customer_id'].iloc[-1]}"
        )

        self.dimensions["dim_customer"] = customer_dim
        return customer_dim

    def build_product_dimension(self, product_df):
        """Create product dimension table"""
        logger.info("Building product dimension...")

        # Create product dimension
        product_dim = product_df.copy()

        # Ensure required columns
        required_mapping = {
            "product_name": "Unnamed Product",
            "category": "General",
            "sub_category": "General",
            "brand": "Default Brand",
            "cost_price": 0,
            "selling_price": 0,
            "supplier": "Default Supplier",
            "is_active": True,
        }

        for col, default_value in required_mapping.items():
            if col not in product_dim.columns:
                if col in ["cost_price", "selling_price"]:
                    # For prices, use base_price if available
                    if "base_price" in product_dim.columns:
                        if col == "cost_price":
                            product_dim[col] = product_dim["base_price"] * 0.7
                        else:
                            product_dim[col] = product_dim["base_price"]
                    else:
                        product_dim[col] = default_value
                else:
                    product_dim[col] = default_value

        # Ensure product_id is string for consistent joining
        product_dim["product_id"] = product_dim["product_id"].astype(str)

        # Calculate profit margin if we have both prices
        if (
            "cost_price" in product_dim.columns
            and "selling_price" in product_dim.columns
        ):
            # Avoid division by zero
            mask = product_dim["selling_price"] > 0
            product_dim.loc[mask, "profit_margin"] = (
                (
                    product_dim.loc[mask, "selling_price"]
                    - product_dim.loc[mask, "cost_price"]
                )
                / product_dim.loc[mask, "selling_price"]
                * 100
            ).round(2)
            product_dim.loc[~mask, "profit_margin"] = 0
        else:
            product_dim["profit_margin"] = 30.0  # Default

        logger.info(f"✅ Product dimension created: {len(product_dim)} products")
        logger.info(
            f"   Product IDs: {product_dim['product_id'].iloc[0]} to {product_dim['product_id'].iloc[-1]}"
        )

        self.dimensions["dim_product"] = product_dim
        return product_dim

    def get_existing_salespersons(self, db_connection):
        """Get existing salespersons from database"""
        logger.info("Fetching existing salespersons from database...")

        try:
            cursor = db_connection.cursor(
                dictionary=True
            )  # This should be indented 8 spaces
            cursor.execute("""
                SELECT salesperson_key, salesperson_id, salesperson_name, 
                     email, region, territory, manager_id, hire_date, commission_rate 
                FROM dim_salesperson
            """)
            salespersons = cursor.fetchall()
            cursor.close()

            if salespersons:
                salesperson_df = pd.DataFrame(salespersons)
                logger.info(f"✅ Found {len(salesperson_df)} salespersons in database")
                return salesperson_df
            else:
                logger.warning("No salespersons found in database, creating defaults")
                return self._create_default_salespersons()

        except Exception as e:
            logger.error(f"Error fetching salespersons: {e}")
            return self._create_default_salespersons()

    def _create_default_salespersons(self):
        """Create default salesperson dimension"""
        default_salespersons = [
            {
                "salesperson_id": "SP001",
                "first_name": "John",
                "last_name": "Smith",
                "region": "North",
                "email": "john.smith@company.com",
            },
            {
                "salesperson_id": "SP002",
                "first_name": "Sarah",
                "last_name": "Johnson",
                "region": "South",
                "email": "sarah.johnson@company.com",
            },
            {
                "salesperson_id": "SP003",
                "first_name": "Mike",
                "last_name": "Brown",
                "region": "East",
                "email": "mike.brown@company.com",
            },
            {
                "salesperson_id": "SP004",
                "first_name": "Lisa",
                "last_name": "Davis",
                "region": "West",
                "email": "lisa.davis@company.com",
            },
        ]

        salesperson_df = pd.DataFrame(default_salespersons)

        # Add employee_id if needed (using same as salesperson_id)
        salesperson_df["employee_id"] = salesperson_df["salesperson_id"]

        # Add salesperson_name
        salesperson_df["salesperson_name"] = (
            salesperson_df["first_name"] + " " + salesperson_df["last_name"]
        )

        # Add additional columns
        salesperson_df["territory"] = salesperson_df["region"]
        salesperson_df["manager_id"] = "MGR001"
        salesperson_df["hire_date"] = pd.Timestamp("2022-01-01")
        salesperson_df["commission_rate"] = 0.10

        # Note: salesperson_key will be auto-generated by MySQL
        # Do NOT add salesperson_key column here

        logger.info(
            f"✅ Created default salesperson dimension with {len(salesperson_df)} salespersons"
        )
        logger.info(f"   Salesperson IDs: {salesperson_df['salesperson_id'].tolist()}")

        self.dimensions["dim_salesperson"] = salesperson_df
        return salesperson_df

    def get_all_dimensions(self):
        """Get all built dimension tables"""
        return self.dimensions

    def save_dimensions(self, output_dir="data/processed"):
        """Save dimension tables to CSV files"""
        import os

        os.makedirs(output_dir, exist_ok=True)

        for dim_name, df in self.dimensions.items():
            filename = f"{output_dir}/{dim_name}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"✅ Saved {dim_name} to {filename} ({len(df)} rows)")

        return True

    def validate_dimensions(self, sales_df):
        """Validate that dimensions properly cover sales data"""
        logger.info("Validating dimensions against sales data...")

        validation_results = {}

        # Check date dimension
        if "dim_date" in self.dimensions:
            date_dim = self.dimensions["dim_date"]
            sales_dates = set(pd.to_datetime(sales_df["sale_date"]).dt.date.unique())
            dim_dates = set(date_dim["full_date"])

            missing_dates = sales_dates - dim_dates
            validation_results["date_coverage"] = {
                "total_sales_dates": len(sales_dates),
                "covered_dates": len(sales_dates - missing_dates),
                "missing_dates": len(missing_dates),
                "missing_dates_list": (
                    sorted(missing_dates)[:10] if missing_dates else []
                ),
            }

            if missing_dates:
                logger.warning(f"⚠️  Date dimension missing {len(missing_dates)} dates")
            else:
                logger.info("✅ Date dimension covers all sales dates")

        # Check customer dimension
        if "dim_customer" in self.dimensions:
            customer_dim = self.dimensions["dim_customer"]
            sales_customers = set(sales_df["customer_id"].astype(str).unique())
            dim_customers = set(customer_dim["customer_id"].astype(str).unique())

            missing_customers = sales_customers - dim_customers
            validation_results["customer_coverage"] = {
                "total_sales_customers": len(sales_customers),
                "covered_customers": len(sales_customers - missing_customers),
                "missing_customers": len(missing_customers),
            }

            if missing_customers:
                logger.warning(
                    f"⚠️  Customer dimension missing {len(missing_customers)} customers"
                )
            else:
                logger.info("✅ Customer dimension covers all sales customers")

        # Check product dimension
        if "dim_product" in self.dimensions:
            product_dim = self.dimensions["dim_product"]
            sales_products = set(sales_df["product_id"].astype(str).unique())
            dim_products = set(product_dim["product_id"].astype(str).unique())

            missing_products = sales_products - dim_products
            validation_results["product_coverage"] = {
                "total_sales_products": len(sales_products),
                "covered_products": len(sales_products - missing_products),
                "missing_products": len(missing_products),
            }

            if missing_products:
                logger.warning(
                    f"⚠️  Product dimension missing {len(missing_products)} products"
                )
            else:
                logger.info("✅ Product dimension covers all sales products")

        return validation_results


# Test function
def test_dimension_builder():
    """Test dimension builder with sample data"""
    print("Testing Dimension Builder...")

    # Sample data
    sample_sales = pd.DataFrame(
        {
            "sale_date": pd.date_range("2024-01-01", periods=10, freq="D"),
            "product_id": ["P001"] * 10,
            "customer_id": ["CUST001"] * 10,
        }
    )

    sample_customers = pd.DataFrame(
        {
            "customer_id": ["CUST001", "CUST002"],
            "customer_name": ["John Doe", "Jane Smith"],
            "email": ["john@example.com", "jane@example.com"],
            "city": ["New York", "Los Angeles"],
            "state": ["NY", "CA"],
            "country": ["USA", "USA"],
            "region": ["North", "West"],
        }
    )

    sample_products = pd.DataFrame(
        {
            "product_id": ["P001", "P002"],
            "product_name": ["Laptop", "Mouse"],
            "category": ["Electronics", "Accessories"],
            "sub_category": ["Computers", "Peripherals"],
            "base_price": [1000, 50],
        }
    )

    builder = DimensionBuilder()

    # Build dimensions
    date_dim = builder.build_date_dimension(sample_sales)
    customer_dim = builder.build_customer_dimension(sample_customers)
    product_dim = builder.build_product_dimension(sample_products)

    print(f"Date dimension: {len(date_dim)} rows")
    print(f"  Range: {date_dim['full_date'].min()} to {date_dim['full_date'].max()}")
    print(f"Customer dimension: {len(customer_dim)} rows")
    print(f"Product dimension: {len(product_dim)} rows")

    # Validate
    validation = builder.validate_dimensions(sample_sales)
    print("\nValidation Results:")
    for dim, results in validation.items():
        print(f"  {dim}: {results}")

    print("✅ Test completed")


if __name__ == "__main__":
    test_dimension_builder()
