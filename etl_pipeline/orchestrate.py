"""
Main ETL Orchestrator - Run the complete pipeline - FIXED VERSION
"""
import sys
import os
import pandas as pd
import logging
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import our modules
from etl_pipeline.config.database_config import get_simple_connection
from etl_pipeline.transform.data_cleaner import DataCleaner
from etl_pipeline.transform.dimension_builder import DimensionBuilder
from etl_pipeline.transform.fact_table_creator import FactTableCreator
from etl_pipeline.load.mysql_loader import MySQLLoader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('etl_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)

class ETLOrchestrator:
    """Orchestrate the complete ETL pipeline - FIXED VERSION"""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.stats = {}
        
    def run_etl_pipeline(self):
        """Execute the complete ETL pipeline - FIXED LOADING ORDER"""
        self.start_time = datetime.now()
        
        logger.info("=" * 70)
        logger.info("🚀 STARTING ETL PIPELINE - FIXED VERSION")
        logger.info("=" * 70)
        logger.info(f"Start time: {self.start_time}")
        
        # Track success of each step
        success_flags = {
            'extract': False,
            'clean': False,
            'dimensions': False,
            'fact': False,
            'load': False,
            'verify': False
        }
        
        try:
            # Step 1: Extract - Read data from CSV files
            logger.info("\n📥 STEP 1: EXTRACTING DATA")
            logger.info("-" * 40)
            
            raw_data = self._extract_data()
            if raw_data:
                success_flags['extract'] = True
                logger.info(f"✅ Extracted {len(raw_data['sales'])} sales, {len(raw_data['customers'])} customers, {len(raw_data['products'])} products")
            else:
                logger.error("❌ Failed to extract data")
                return False
            
            # Step 2: Transform - Clean and validate data
            logger.info("\n🔧 STEP 2: CLEANING AND TRANSFORMING DATA")
            logger.info("-" * 40)
            
            cleaned_data = self._transform_data(raw_data)
            if cleaned_data:
                success_flags['clean'] = True
                logger.info("✅ Data cleaning completed")
            else:
                logger.error("❌ Data cleaning failed")
                return False
            
            # Step 3: Build dimension tables
            logger.info("\n🏗️ STEP 3: BUILDING DIMENSION TABLES")
            logger.info("-" * 40)
            
            dimensions = self._build_dimensions(cleaned_data)
            if dimensions:
                success_flags['dimensions'] = True
                logger.info(f"✅ Built {len(dimensions)} dimension tables")
            else:
                logger.error("❌ Dimension building failed")
                return False
            
            # Step 4: Create fact table
            logger.info("\n🎯 STEP 4: CREATING FACT TABLE")
            logger.info("-" * 40)
            
            fact_table = self._create_fact_table(cleaned_data['sales'], dimensions)
            if fact_table is not None:
                success_flags['fact'] = True
                logger.info(f"✅ Fact table created with {len(fact_table)} rows")
            else:
                logger.error("❌ Fact table creation failed")
                return False
            
            # Step 5: Load data into MySQL - IN CORRECT ORDER
            logger.info("\n📤 STEP 5: LOADING DATA TO DATABASE")
            logger.info("-" * 40)
            
            load_success = self._load_to_database_safely(dimensions, fact_table)
            if load_success:
                success_flags['load'] = True
                logger.info("✅ Data loaded to database")
            else:
                logger.error("❌ Data loading failed")
                return False
            
            # Step 6: Verify data load
            logger.info("\n✅ STEP 6: VERIFYING DATA LOAD")
            logger.info("-" * 40)
            
            verify_success = self._verify_data_load()
            if verify_success:
                success_flags['verify'] = True
                logger.info("✅ Data verification passed")
            else:
                logger.warning("⚠️  Data verification found issues")
            
            # Calculate statistics
            self.end_time = datetime.now()
            duration = self.end_time - self.start_time
            
            # Final report
            logger.info("\n" + "=" * 70)
            logger.info("📊 ETL PIPELINE EXECUTION REPORT")
            logger.info("=" * 70)
            
            logger.info(f"Start Time: {self.start_time}")
            logger.info(f"End Time: {self.end_time}")
            logger.info(f"Total Duration: {duration}")
            
            logger.info("\nStep Summary:")
            for step, success in success_flags.items():
                status = "✅ PASSED" if success else "❌ FAILED"
                logger.info(f"  {step:15}: {status}")
            
            logger.info("\nData Statistics:")
            for stat_name, stat_value in self.stats.items():
                logger.info(f"  {stat_name:30}: {stat_value}")
            
            all_success = all(success_flags.values())
            if all_success:
                logger.info("\n🎉 ETL PIPELINE COMPLETED SUCCESSFULLY!")
            else:
                logger.warning("\n⚠️  ETL PIPELINE COMPLETED WITH ISSUES")
            
            logger.info("=" * 70)
            
            return all_success
            
        except Exception as e:
            logger.error(f"❌ ETL Pipeline failed with error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _extract_data(self):
        """Extract data from CSV files"""
        try:
            data_dir = 'data/raw'
            
            # Check if files exist
            required_files = ['sales_raw.csv', 'customers_raw.csv', 'products_raw.csv']
            for file in required_files:
                filepath = os.path.join(data_dir, file)
                if not os.path.exists(filepath):
                    logger.error(f"Missing file: {filepath}")
                    return None
            
            # Read CSV files
            sales_df = pd.read_csv(os.path.join(data_dir, 'sales_raw.csv'))
            customers_df = pd.read_csv(os.path.join(data_dir, 'customers_raw.csv'))
            products_df = pd.read_csv(os.path.join(data_dir, 'products_raw.csv'))
            
            # Store stats
            self.stats['Raw Sales Records'] = len(sales_df)
            self.stats['Raw Customer Records'] = len(customers_df)
            self.stats['Raw Product Records'] = len(products_df)
            
            return {
                'sales': sales_df,
                'customers': customers_df,
                'products': products_df
            }
            
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            return None
    
    def _transform_data(self, raw_data):
        """Clean and transform raw data"""
        try:
            cleaner = DataCleaner()
            
            # Clean each dataset
            cleaned_sales = cleaner.clean_sales_data(raw_data['sales'])
            cleaned_customers = cleaner.clean_customer_data(raw_data['customers'])
            cleaned_products = cleaner.clean_product_data(raw_data['products'])
            
            # Validate data quality
            cleaner.validate_data_quality()
            
            # Save cleaned data (optional)
            cleaner.save_cleaned_data('data/processed')
            
            # Store stats
            self.stats['Cleaned Sales Records'] = len(cleaned_sales)
            self.stats['Cleaned Customer Records'] = len(cleaned_customers)
            self.stats['Cleaned Product Records'] = len(cleaned_products)
            
            return {
                'sales': cleaned_sales,
                'customers': cleaned_customers,
                'products': cleaned_products
            }
            
        except Exception as e:
            logger.error(f"Error transforming data: {e}")
            return None
    
    def _build_dimensions(self, cleaned_data):
        """Build dimension tables"""
        try:
            builder = DimensionBuilder()
            
            # Build date dimension from sales data
            date_dim = builder.build_date_dimension(cleaned_data['sales'])
            
            # Build customer dimension
            customer_dim = builder.build_customer_dimension(cleaned_data['customers'])
            
            # Build product dimension
            product_dim = builder.build_product_dimension(cleaned_data['products'])
            
            # Get salesperson dimension from database
            conn = get_simple_connection()
            salesperson_dim = builder.get_existing_salespersons(conn)
            conn.close()
            
            # Save dimensions (optional)
            builder.save_dimensions('data/processed')
            
            # Store stats
            self.stats['Date Dimension Rows'] = len(date_dim)
            self.stats['Customer Dimension Rows'] = len(customer_dim)
            self.stats['Product Dimension Rows'] = len(product_dim)
            self.stats['Salesperson Dimension Rows'] = len(salesperson_dim)
            
            return {
                'dim_date': date_dim,
                'dim_customer': customer_dim,
                'dim_product': product_dim,
                'dim_salesperson': salesperson_dim
            }
            
        except Exception as e:
            logger.error(f"Error building dimensions: {e}")
            return None
    
    def _create_fact_table(self, sales_df, dimensions):
        """Create fact table"""
        try:
            creator = FactTableCreator()
            
            # Get database connection for salesperson mapping
            conn = get_simple_connection()
            
            # Create fact table
            fact_table = creator.create_fact_table(sales_df, dimensions, conn)
            
            conn.close()
            
            # Save fact table (optional)
            if fact_table is not None:
                creator.save_fact_table('data/processed/fact_sales.csv')
                
                # Store stats
                self.stats['Fact Table Rows'] = len(fact_table)
                self.stats['Total Revenue'] = f"${fact_table['total_amount'].sum():,.2f}"
                self.stats['Total Profit'] = f"${fact_table['profit'].sum():,.2f}"
                self.stats['Avg Order Value'] = f"${fact_table['total_amount'].mean():,.2f}"
            
            return fact_table
            
        except Exception as e:
            logger.error(f"Error creating fact table: {e}")
            return None
    
    def _load_to_database_safely(self, dimensions, fact_table):
        """Load data into MySQL database - FIXED VERSION with safe order"""
        try:
            loader = MySQLLoader()
            
            # STEP 1: Clear all tables in safe order
            logger.info("Step 1: Clearing all tables in safe order...")
            clear_success = loader.clear_all_tables_safely()
            if not clear_success:
                logger.error("Failed to clear tables")
                return False
            
            # STEP 2: Load dimension tables (order doesn't matter now since all cleared)
            logger.info("Step 2: Loading dimension tables...")
            
            dimension_loaders = [
                ('dim_date', dimensions['dim_date']),
                ('dim_customer', dimensions['dim_customer']),
                ('dim_product', dimensions['dim_product']),
                ('dim_salesperson', dimensions['dim_salesperson'])
            ]
            
            for table_name, df in dimension_loaders:
                # Don't truncate since we already cleared
                success = loader.load_dimension_table(df, table_name, truncate_first=False)
                if not success:
                    logger.error(f"Failed to load {table_name}")
                    return False
            
            # STEP 3: Load fact table (don't truncate since we already cleared)
            logger.info("Step 3: Loading fact table...")
            success = loader.load_fact_table(fact_table, 'fact_sales', truncate_first=False)
            if not success:
                logger.error("Failed to load fact table")
                return False
            
            loader.close_connection()
            return True
            
        except Exception as e:
            logger.error(f"Error loading to database: {e}")
            return False
    
    def _verify_data_load(self):
        """Verify data was loaded correctly"""
        try:
            loader = MySQLLoader()
            success = loader.verify_data_load()
            loader.close_connection()
            return success
        except Exception as e:
            logger.error(f"Error verifying data load: {e}")
            return False

def main():
    """Main function to run ETL pipeline"""
    orchestrator = ETLOrchestrator()
    success = orchestrator.run_etl_pipeline()
    
    if success:
        print("\n" + "=" * 70)
        print("🎉 ETL PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        print("\n📊 Data loaded into MySQL tables:")
        print("   • dim_date: Date dimension")
        print("   • dim_customer: Customer dimension")
        print("   • dim_product: Product dimension")
        print("   • dim_salesperson: Sales team dimension")
        print("   • fact_sales: Sales transactions")
        print("\n✅ Next steps:")
        print("   1. Run analytics queries")
        print("   2. Launch Streamlit dashboard")
        print("   3. Check the log file: etl_pipeline.log")
    else:
        print("\n" + "=" * 70)
        print("❌ ETL PIPELINE FAILED")
        print("=" * 70)
        print("\n🔍 Check the log file for details: etl_pipeline.log")
        print("💡 Common issues:")
        print("   • MySQL service not running")
        print("   • Incorrect database credentials in .env file")
        print("   • Missing CSV files in data/raw/ folder")
    
    return success

if __name__ == "__main__":
    # Run the ETL pipeline
    success = main()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)