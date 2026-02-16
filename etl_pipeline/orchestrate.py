"""
Main ETL Orchestrator
Runs the complete ETL pipeline end-to-end
"""

import sys
import os
import logging
import pandas as pd
from datetime import datetime

# Ensure project root is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Imports
from etl_pipeline.config.database_config import get_simple_connection
from etl_pipeline.transform.data_cleaner import DataCleaner
from etl_pipeline.transform.dimension_builder import DimensionBuilder
from etl_pipeline.transform.fact_table_creator import FactTableCreator
from etl_pipeline.load.mysql_loader import MySQLLoader

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("etl_pipeline.log"),
    ],
)
logger = logging.getLogger(__name__)


class ETLOrchestrate:
    """Orchestrates the complete ETL pipeline"""

    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.stats = {}

    # ============================================================
    # MAIN PIPELINE
    # ============================================================
    def run_etl_pipeline(self):
        self.start_time = datetime.now()

        logger.info("=" * 70)
        logger.info("🚀 STARTING ETL PIPELINE")
        logger.info("=" * 70)
        logger.info(f"Start time: {self.start_time}")

        try:
            raw_data = self._extract_data()
            cleaned_data = self._transform_data(raw_data)
            dimensions = self._build_dimensions(cleaned_data)
            fact_table = self._create_fact_table(cleaned_data["sales"], dimensions)
            self._load_to_database(dimensions, fact_table)

            self.end_time = datetime.now()
            self._final_report()

            return True

        except Exception as e:
            logger.error(f"❌ ETL PIPELINE FAILED: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    # ============================================================
    # STEP 1: EXTRACT
    # ============================================================
    def _extract_data(self):
        logger.info("\n📥 STEP 1: EXTRACTING DATA")
        data_dir = "data/raw"

        sales = pd.read_csv(os.path.join(data_dir, "sales_raw.csv"))
        customers = pd.read_csv(os.path.join(data_dir, "customers_raw.csv"))
        products = pd.read_csv(os.path.join(data_dir, "products_raw.csv"))

        self.stats["Raw Sales"] = len(sales)
        self.stats["Raw Customers"] = len(customers)
        self.stats["Raw Products"] = len(products)

        logger.info("✅ Data extracted")
        return {
            "sales": sales,
            "customers": customers,
            "products": products,
        }

    # ============================================================
    # STEP 2: TRANSFORM
    # ============================================================
    def _transform_data(self, raw_data):
        logger.info("\n🔧 STEP 2: CLEANING DATA")

        cleaner = DataCleaner()

        sales = cleaner.clean_sales_data(raw_data["sales"])
        customers = cleaner.clean_customer_data(raw_data["customers"])
        products = cleaner.clean_product_data(raw_data["products"])

        cleaner.validate_data_quality()
        cleaner.save_cleaned_data("data/processed")

        self.stats["Cleaned Sales"] = len(sales)
        self.stats["Cleaned Customers"] = len(customers)
        self.stats["Cleaned Products"] = len(products)

        logger.info("✅ Data cleaned")
        return {
            "sales": sales,
            "customers": customers,
            "products": products,
        }

    # ============================================================
    # STEP 3: DIMENSIONS
    # ============================================================
    def _build_dimensions(self, cleaned_data):
        logger.info("\n🏗️ STEP 3: BUILDING DIMENSIONS")

        builder = DimensionBuilder()

        dim_date = builder.build_date_dimension(cleaned_data["sales"])
        dim_customer = builder.build_customer_dimension(cleaned_data["customers"])
        dim_product = builder.build_product_dimension(cleaned_data["products"])

        conn = get_simple_connection()
        dim_salesperson = builder.get_existing_salespersons(conn)
        conn.close()

        builder.save_dimensions("data/processed")

        self.stats["Dim Date"] = len(dim_date)
        self.stats["Dim Customer"] = len(dim_customer)
        self.stats["Dim Product"] = len(dim_product)
        self.stats["Dim Salesperson"] = len(dim_salesperson)

        logger.info("✅ Dimensions built")

        return {
            "dim_date": dim_date,
            "dim_customer": dim_customer,
            "dim_product": dim_product,
            "dim_salesperson": dim_salesperson,
        }

    # ============================================================
    # STEP 4: FACT TABLE
    # ============================================================
    def _create_fact_table(self, sales_df, dimensions):
        logger.info("\n🎯 STEP 4: CREATING FACT TABLE")

        creator = FactTableCreator()
        conn = get_simple_connection()

        fact = creator.create_fact_table(sales_df, dimensions, conn)
        conn.close()

        creator.save_fact_table("data/processed/fact_sales.csv")

        self.stats["Fact Rows"] = len(fact)
        self.stats["Total Revenue"] = f"${fact['total_amount'].sum():,.2f}"
        self.stats["Total Profit"] = f"${fact['profit'].sum():,.2f}"

        logger.info("✅ Fact table created")
        return fact

    # ============================================================
    # STEP 5: LOAD TO MYSQL
    # ============================================================
    def _load_to_database(self, dimensions, fact_table):
        logger.info("\n📤 STEP 5: LOADING DATA TO MYSQL")

        loader = MySQLLoader()

        # Clear tables ONCE
        loader.clear_all_tables_safely()

        # Load dimensions
        loader.load_dimension_table(dimensions["dim_date"], "dim_date")
        loader.load_dimension_table(dimensions["dim_customer"], "dim_customer")
        loader.load_dimension_table(dimensions["dim_product"], "dim_product")
        loader.load_dimension_table(dimensions["dim_salesperson"], "dim_salesperson")

        # Load fact
        loader.load_fact_table(fact_table, "fact_sales")

        loader.close_connection()
        logger.info("✅ Database load complete")

    # ============================================================
    # FINAL REPORT
    # ============================================================
    def _final_report(self):
        duration = self.end_time - self.start_time

        logger.info("\n" + "=" * 70)
        logger.info("📊 ETL EXECUTION REPORT")
        logger.info("=" * 70)
        logger.info(f"Start: {self.start_time}")
        logger.info(f"End:   {self.end_time}")
        logger.info(f"Time:  {duration}")

        for k, v in self.stats.items():
            logger.info(f"{k:25}: {v}")

        logger.info("\n🎉 ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)


# ============================================================
# ENTRY POINT
# ============================================================
def main():
    etl = ETLOrchestrate()
    success = etl.run_etl_pipeline()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
