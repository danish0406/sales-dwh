"""
Simplified ETL Pipeline - Fixes foreign key issues
"""
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def get_db_connection():
    """Get MySQL connection"""
    try:
        conn = mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', 3306)),
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', ''),
            database=os.getenv('DB_NAME', 'sales_dwh'),
            autocommit=True
        )
        return conn
    except Error as e:
        logger.error(f"Database connection error: {e}")
        return None

def clear_all_tables():
    """Clear all tables in correct order"""
    logger.info("Clearing all tables...")
    
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        # Disable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        # Clear in correct order
        tables = ['fact_sales', 'staging_sales', 'dim_date', 'dim_customer', 'dim_product', 'dim_salesperson']
        
        for table in tables:
            try:
                cursor.execute(f"DELETE FROM {table}")
                cursor.execute(f"ALTER TABLE {table} AUTO_INCREMENT = 1")
                logger.info(f"  Cleared {table}")
            except Exception as e:
                logger.warning(f"  Could not clear {table}: {e}")
        
        # Re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        conn.commit()
        
        logger.info("✅ All tables cleared")
        return True
        
    except Exception as e:
        logger.error(f"Error clearing tables: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def load_dimension_from_csv(csv_file, table_name, id_column):
    """
    Load dimension table from CSV, letting MySQL auto-assign keys
    Returns: Dictionary mapping original IDs to database keys
    """
    logger.info(f"Loading {table_name} from {csv_file}...")
    
    # Read CSV
    df = pd.read_csv(csv_file)
    
    conn = get_db_connection()
    if not conn:
        return None
    
    cursor = conn.cursor()
    
    try:
        # Get column names from the table (excluding auto-increment key)
        cursor.execute(f"DESCRIBE {table_name}")
        table_info = cursor.fetchall()
        
        # Get data columns (not ending with '_key')
        data_columns = [col[0] for col in table_info if not col[0].endswith('_key')]
        
        # Prepare insert statement
        placeholders = ', '.join(['%s'] * len(data_columns))
        columns = ', '.join(data_columns)
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Prepare data
        data_to_insert = []
        for _, row in df.iterrows():
            row_data = []
            for col in data_columns:
                if col in df.columns:
                    val = row[col]
                    row_data.append(None if pd.isna(val) else val)
                else:
                    row_data.append(None)
            data_to_insert.append(tuple(row_data))
        
        # Insert data
        for row_data in data_to_insert:
            cursor.execute(insert_query, row_data)
        
        conn.commit()
        
        # Get the mapping of original IDs to database keys
        cursor.execute(f"SELECT {id_column}, {table_name.replace('dim_', '')}_key FROM {table_name}")
        mapping = {row[0]: row[1] for row in cursor.fetchall()}
        
        logger.info(f"✅ Loaded {len(df)} rows into {table_name}")
        return mapping
        
    except Exception as e:
        logger.error(f"Error loading {table_name}: {e}")
        conn.rollback()
        return None
    finally:
        cursor.close()
        conn.close()

def create_date_dimension(sales_df):
    """Create date dimension from sales data"""
    logger.info("Creating date dimension...")
    
    conn = get_db_connection()
    if not conn:
        return None
    
    cursor = conn.cursor()
    
    try:
        # Extract unique dates from sales
        sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
        unique_dates = sales_df['sale_date'].dt.date.unique()
        
        # Clear existing dates
        cursor.execute("DELETE FROM dim_date")
        cursor.execute("ALTER TABLE dim_date AUTO_INCREMENT = 1")
        
        # Insert dates
        for date_val in sorted(unique_dates):
            date_obj = pd.Timestamp(date_val)
            cursor.execute("""
                INSERT INTO dim_date (full_date, year, quarter, month, month_name, day, day_of_week, is_weekend)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                date_val,
                date_obj.year,
                date_obj.quarter,
                date_obj.month,
                date_obj.strftime('%B'),
                date_obj.day,
                date_obj.strftime('%A'),
                date_obj.dayofweek >= 5
            ))
        
        conn.commit()
        
        # Get date_key mapping
        cursor.execute("SELECT full_date, date_key FROM dim_date")
        date_mapping = {str(row[0]): row[1] for row in cursor.fetchall()}
        
        logger.info(f"✅ Created date dimension with {len(unique_dates)} dates")
        return date_mapping
        
    except Exception as e:
        logger.error(f"Error creating date dimension: {e}")
        conn.rollback()
        return None
    finally:
        cursor.close()
        conn.close()

def load_fact_table(sales_df, date_mapping, customer_mapping, product_mapping):
    """Load fact table with proper foreign keys"""
    logger.info(f"Loading fact table with {len(sales_df)} rows...")
    
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        # Clear fact table
        cursor.execute("DELETE FROM fact_sales")
        cursor.execute("ALTER TABLE fact_sales AUTO_INCREMENT = 1")
        
        # Prepare insert statement
        insert_query = """
            INSERT INTO fact_sales 
            (date_key, product_key, customer_key, salesperson_key, 
             quantity, unit_price, discount, total_amount, profit, 
             payment_method, shipping_mode)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Process sales data
        batch_size = 100
        batch = []
        
        for _, row in sales_df.iterrows():
            try:
                # Convert sale_date to string for mapping
                sale_date = pd.to_datetime(row['sale_date']).date()
                date_str = str(sale_date)
                
                # Get foreign keys from mappings
                date_key = date_mapping.get(date_str)
                customer_key = customer_mapping.get(row['customer_id'])
                product_key = product_mapping.get(row['product_id'])
                
                # Skip if any foreign key is missing
                if not all([date_key, customer_key, product_key]):
                    logger.warning(f"Skipping sale {row.get('sale_id', 'unknown')}: missing foreign keys")
                    continue
                
                # Default salesperson (simplified)
                salesperson_key = 1
                
                # Calculate profit (simplified: 20% of total)
                profit = row['total_amount'] * 0.2
                
                batch.append((
                    date_key, product_key, customer_key, salesperson_key,
                    row['quantity'], row['unit_price'], row.get('discount', 0),
                    row['total_amount'], profit,
                    row.get('payment_method', 'Credit Card'),
                    row.get('shipping_mode', 'Standard')
                ))
                
                # Insert in batches
                if len(batch) >= batch_size:
                    cursor.executemany(insert_query, batch)
                    batch = []
                    
            except Exception as e:
                logger.warning(f"Error processing sale: {e}")
                continue
        
        # Insert remaining rows
        if batch:
            cursor.executemany(insert_query, batch)
        
        conn.commit()
        
        # Get count
        cursor.execute("SELECT COUNT(*) FROM fact_sales")
        count = cursor.fetchone()[0]
        
        logger.info(f"✅ Loaded {count} rows into fact_sales")
        return True
        
    except Exception as e:
        logger.error(f"Error loading fact table: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def run_simple_etl():
    """Run simplified ETL pipeline"""
    logger.info("=" * 60)
    logger.info("🚀 SIMPLIFIED ETL PIPELINE")
    logger.info("=" * 60)
    
    try:
        # Step 1: Clear all tables
        if not clear_all_tables():
            return False
        
        # Step 2: Load dimensions and get key mappings
        logger.info("\n📥 Loading dimensions...")
        
        # Load customers
        customer_mapping = load_dimension_from_csv(
            'data/raw/customers_raw.csv',
            'dim_customer',
            'customer_id'
        )
        if not customer_mapping:
            return False
        
        # Load products
        product_mapping = load_dimension_from_csv(
            'data/raw/products_raw.csv',
            'dim_product',
            'product_id'
        )
        if not product_mapping:
            return False
        
        # Step 3: Read sales data
        logger.info("\n📊 Processing sales data...")
        sales_df = pd.read_csv('data/raw/sales_raw.csv')
        
        # Step 4: Create date dimension from sales dates
        date_mapping = create_date_dimension(sales_df)
        if not date_mapping:
            return False
        
        # Step 5: Load fact table with proper mappings
        logger.info("\n🎯 Loading fact table...")
        success = load_fact_table(sales_df, date_mapping, customer_mapping, product_mapping)
        
        if success:
            logger.info("\n" + "=" * 60)
            logger.info("🎉 SIMPLIFIED ETL COMPLETED SUCCESSFULLY!")
            logger.info("=" * 60)
            
            # Show summary
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute("SELECT 'dim_date' as table, COUNT(*) as count FROM dim_date")
            cursor.execute("SELECT 'dim_customer' as table, COUNT(*) as count FROM dim_customer")
            cursor.execute("SELECT 'dim_product' as table, COUNT(*) as count FROM dim_product")
            cursor.execute("SELECT 'fact_sales' as table, COUNT(*) as count FROM fact_sales")
            
            tables = ['dim_date', 'dim_customer', 'dim_product', 'fact_sales']
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                result = cursor.fetchone()
                logger.info(f"  {table:15}: {result['count']:6} rows")
            
            cursor.close()
            conn.close()
            
            return True
        else:
            return False
        
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    

class MySQLLoader:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def run_etl(self):
        """Run the ETL pipeline"""
        return run_simple_etl()    

if __name__ == "__main__":
    success = run_simple_etl()
    
    print("\n" + "=" * 60)
    if success:
        print("✅ ETL COMPLETED!")
        print("\n📊 Check your data in MySQL:")
        print("   mysql -u root -p -e \"USE sales_dwh; SELECT * FROM fact_sales LIMIT 5;\"")
    else:
        print("❌ ETL FAILED")
        print("Check the error messages above.")
    
    exit(0 if success else 1)