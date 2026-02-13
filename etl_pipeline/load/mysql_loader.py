"""
MySQL Loader Module - Handles loading data into MySQL database
"""

import mysql.connector
from mysql.connector import Error
import pandas as pd  # <-- ADD THIS IMPORT
import logging
from datetime import datetime

# Setup logging
logger = logging.getLogger(__name__)


class MySQLLoader:
    """MySQL Loader class for ETL pipeline"""

    def __init__(self):
        """Initialize the MySQL loader"""
        self.logger = logging.getLogger(__name__)
        self.connection = None
        self.cursor = None
        self._connect()

    def _connect(self):
        """Establish database connection"""
        try:
            from etl_pipeline.config.database_config import get_simple_connection

            self.connection = get_simple_connection()
            if self.connection:
                self.cursor = self.connection.cursor(dictionary=True)
                self.logger.info("✅ MySQL connection established")
            else:
                self.logger.error("❌ Failed to connect to MySQL")
        except Exception as e:
            self.logger.error(f"❌ Connection error: {e}")

    def clear_all_tables_safely(self):
        """Clear all tables in correct order - handles foreign keys"""
        self.logger.info("Clearing all tables safely...")

        if not self.connection or not self.cursor:
            self.logger.error("No database connection")
            return False

        try:
            # Disable foreign key checks
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

            # Get all tables
            self.cursor.execute("SHOW TABLES")
            tables_result = self.cursor.fetchall()
            tables = [list(table.values())[0] for table in tables_result]

            # Define clear order (child tables first, then parent tables)
            # Fact tables first, then dimensions
            clear_order = [
                "fact_sales",
                "dim_date",
                "dim_customer",
                "dim_product",
                "dim_salesperson",
            ]

            for table in clear_order:
                if table in tables:
                    try:
                        # Try TRUNCATE first (faster)
                        self.cursor.execute(f"TRUNCATE TABLE {table}")
                        self.logger.info(f"  ✓ Truncated {table}")
                    except:
                        # Fall back to DELETE if TRUNCATE fails
                        try:
                            self.cursor.execute(f"DELETE FROM {table}")
                            self.cursor.execute(
                                f"ALTER TABLE {table} AUTO_INCREMENT = 1"
                            )
                            self.logger.info(f"  ✓ Deleted from {table}")
                        except Exception as e:
                            self.logger.warning(f"  Could not clear {table}: {e}")

            # Also clear any other tables that might exist
            for table in tables:
                if table not in clear_order:
                    try:
                        self.cursor.execute(f"DELETE FROM {table}")
                        self.logger.info(f"  ✓ Cleared additional table: {table}")
                    except:
                        pass

            # Re-enable foreign key checks
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            self.connection.commit()

            self.logger.info("✅ All tables cleared successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error clearing tables: {e}")
            self.connection.rollback()
            return False

    def load_dimension_table(self, df, table_name, truncate_first=False):
        """Load a dimension table"""
        self.logger.info(f"Loading {table_name} with {len(df)} rows...")

        if not self.connection or not self.cursor:
            self.logger.error("No database connection")
            return False

        try:
            # Truncate if requested
            if truncate_first:
                self.cursor.execute(f"DELETE FROM {table_name}")
                self.cursor.execute(f"ALTER TABLE {table_name} AUTO_INCREMENT = 1")
                self.connection.commit()

            # Get table structure
            self.cursor.execute(f"DESCRIBE {table_name}")
            columns_result = self.cursor.fetchall()
            columns = [
                col["Field"]
                for col in columns_result
                if not col["Field"].endswith("_key")
                or col["Field"] == "employee_id"
                or col["Field"] == "customer_id"
                or col["Field"] == "product_id"
            ]

            # Filter dataframe columns that exist in table
            df_columns = [col for col in df.columns if col in columns]

            if not df_columns:
                self.logger.warning(f"No matching columns found for {table_name}")
                return False

            # Prepare insert statement
            placeholders = ", ".join(["%s"] * len(df_columns))
            column_str = ", ".join(df_columns)
            insert_query = (
                f"INSERT INTO {table_name} ({column_str}) VALUES ({placeholders})"
            )

            # Insert data in batches
            batch_size = 100
            total_inserted = 0

            for i in range(0, len(df), batch_size):
                batch = df.iloc[i : i + batch_size]
                batch_values = []

                for _, row in batch.iterrows():
                    values = []
                    for col in df_columns:
                        val = row[col]
                        if pd.isna(val):
                            values.append(None)
                        elif isinstance(val, (int, float)):
                            values.append(val)
                        else:
                            values.append(str(val))
                    batch_values.append(tuple(values))

                self.cursor.executemany(insert_query, batch_values)
                self.connection.commit()
                total_inserted += len(batch)
                self.logger.info(
                    f"  Loaded batch {i//batch_size + 1} ({total_inserted} rows)"
                )

            self.logger.info(
                f"✅ Successfully loaded {total_inserted} rows into {table_name}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Error loading {table_name}: {e}")
            self.connection.rollback()
            return False

    def load_fact_table(self, df, table_name, truncate_first=False):
        """Load fact table with proper foreign key validation"""
        self.logger.info(f"Loading {table_name} with {len(df)} rows...")

        # DEBUG: Print ALL columns in the dataframe
        self.logger.info(f"DataFrame columns: {list(df.columns)}")

        # Define expected columns for fact_sales table
        expected_columns = [
            "date_key",
            "product_key",
            "customer_key",
            "salesperson_key",
            "quantity",
            "unit_price",
            "discount",
            "total_amount",
            "profit",
            "payment_method",
            "shipping_mode",
        ]

    # Check for any unexpected columns and drop them
        unexpected = [col for col in df.columns if col not in expected_columns]
        if unexpected:
            self.logger.warning(f"⚠️ Unexpected columns in fact table: {unexpected}")
            # Keep only expected columns that exist
            cols_to_keep = [col for col in expected_columns if col in df.columns]
            df = df[cols_to_keep].copy()
            self.logger.info(f"✅ After cleanup, columns: {list(df.columns)}")

        if not self.connection or not self.cursor:
            self.logger.error("No database connection")
            return False

        try:
            # Truncate if requested
            if truncate_first:
                self.cursor.execute(f"DELETE FROM {table_name}")
                self.cursor.execute(f"ALTER TABLE {table_name} AUTO_INCREMENT = 1")
                self.connection.commit()

            # ============== DEBUG: Check dimension tables ==============
            self.logger.info("=" * 60)
            self.logger.info("DEBUG: Checking dimension tables in database")
            self.logger.info("=" * 60)
            
            # Check dim_date
            self.cursor.execute("SELECT COUNT(*) as count FROM dim_date")
            date_count = self.cursor.fetchone()['count']
            self.logger.info(f"dim_date has {date_count} rows")
            
            if date_count == 0:
                self.logger.error("dim_date is empty! Cannot load fact table.")
                return False
            
            # Get sample of valid date keys
            self.cursor.execute("SELECT date_key, full_date FROM dim_date LIMIT 10")
            sample_dates = self.cursor.fetchall()
            self.logger.info("Sample date_keys in database:")
            for row in sample_dates:
                self.logger.info(f"  Key: {row['date_key']}, Date: {row['full_date']}")
            
            # Check your fact table's first few date keys
            self.logger.info(f"First 5 date_keys from fact table: {df['date_key'].head(5).tolist()}")
            
            # Verify if first date key exists
            first_date = df['date_key'].iloc[0]
            self.cursor.execute("SELECT COUNT(*) as count FROM dim_date WHERE date_key = %s", (first_date,))
            exists = self.cursor.fetchone()['count'] > 0
            self.logger.info(f"First date_key {first_date} exists in database: {exists}")
            
            if not exists:
                self.logger.error(f"Date key {first_date} not found in dim_date!")
                # Show what dates ARE in the database around that range
                self.cursor.execute("""
                    SELECT date_key, full_date FROM dim_date 
                    WHERE date_key BETWEEN %s AND %s 
                    ORDER BY date_key
                """, (first_date - 100, first_date + 100))
                nearby = self.cursor.fetchall()
                if nearby:
                    self.logger.info(f"Nearby dates in database: {[row['date_key'] for row in nearby[:5]]}")
                else:
                    self.logger.error("No nearby dates found in database!")
            self.logger.info("=" * 60)
            # ============== END DEBUG ==============

            # Get valid dimension keys from the database
            # Get valid date keys as a SET for fast lookup
            self.cursor.execute("SELECT date_key FROM dim_date")
            date_rows = self.cursor.fetchall()
            valid_date_keys = {row['date_key'] for row in date_rows}
            self.logger.info(f"Found {len(valid_date_keys)} valid date keys in database")

            # Get valid product keys as a SET
            self.cursor.execute("SELECT product_key FROM dim_product")
            product_rows = self.cursor.fetchall()
            valid_product_keys = {row['product_key'] for row in product_rows}
            self.logger.info(f"Found {len(valid_product_keys)} valid product keys in database")

            # Get valid customer keys as a SET
            self.cursor.execute("SELECT customer_key FROM dim_customer")
            customer_rows = self.cursor.fetchall()
            valid_customer_keys = {row['customer_key'] for row in customer_rows}
            self.logger.info(f"Found {len(valid_customer_keys)} valid customer keys in database")

            # Get valid salesperson keys as a SET
            self.cursor.execute("SELECT salesperson_key FROM dim_salesperson")
            salesperson_rows = self.cursor.fetchall()
            valid_salesperson_keys = {row['salesperson_key'] for row in salesperson_rows}
            self.logger.info(f"Found {len(valid_salesperson_keys)} valid salesperson keys in database")

            # Prepare data with validated foreign keys
            validated_rows = []
            skipped_rows = 0
            missing_keys = {"date": 0, "product": 0, "customer": 0, "salesperson": 0}

            for idx, row in df.iterrows():
                try:
                    # Handle date_key
                    date_key = row.get("date_key")
                    if pd.isna(date_key):
                        self.logger.warning(f"Row {idx}: No date_key found")
                        skipped_rows += 1
                        missing_keys["date"] += 1
                        continue

                    # Convert to int for comparison
                    date_key = int(date_key)
                    
                    # Check if date_key exists in database (using SET, not dictionary)
                    if date_key not in valid_date_keys:
                        self.logger.warning(f"Row {idx}: Invalid date_key {date_key}")
                        skipped_rows += 1
                        missing_keys["date"] += 1
                        continue

                    # Handle product_key
                    product_key = row.get("product_key")
                    if pd.isna(product_key):
                        self.logger.warning(f"Row {idx}: No product_key found")
                        skipped_rows += 1
                        missing_keys["product"] += 1
                        continue
                    
                    product_key = int(product_key)
                    if product_key not in valid_product_keys:
                        self.logger.warning(f"Row {idx}: Invalid product_key {product_key}")
                        skipped_rows += 1
                        missing_keys["product"] += 1
                        continue

                    # Handle customer_key
                    customer_key = row.get("customer_key")
                    if pd.isna(customer_key):
                        self.logger.warning(f"Row {idx}: No customer_key found")
                        skipped_rows += 1
                        missing_keys["customer"] += 1
                        continue
                    
                    customer_key = int(customer_key)
                    if customer_key not in valid_customer_keys:
                        self.logger.warning(f"Row {idx}: Invalid customer_key {customer_key}")
                        skipped_rows += 1
                        missing_keys["customer"] += 1
                        continue

                    # Handle salesperson_key
                    salesperson_key = row.get("salesperson_key", 1)
                    if pd.isna(salesperson_key):
                        salesperson_key = 1
                    else:
                        salesperson_key = int(salesperson_key)
                    
                    if salesperson_key not in valid_salesperson_keys:
                        self.logger.warning(f"Row {idx}: Invalid salesperson_key {salesperson_key}, using 1")
                        salesperson_key = 1

                    # Build the row with proper keys
                    validated_row = {
                        "date_key": date_key,
                        "product_key": product_key,
                        "customer_key": customer_key,
                        "salesperson_key": salesperson_key,
                        "quantity": float(row.get("quantity", 0)),
                        "unit_price": float(row.get("unit_price", 0)),
                        "discount": float(row.get("discount", 0)),
                        "total_amount": float(row.get("total_amount", 0)),
                        "profit": float(row.get("profit", 0)),
                    }

                    # Add any additional columns that might exist
                    for col in ["payment_method", "shipping_mode"]:
                        if col in row and pd.notna(row[col]):
                            validated_row[col] = str(row[col])
                        else:
                            # Set defaults
                            if col == "payment_method":
                                validated_row[col] = "Credit Card"
                            elif col == "shipping_mode":
                                validated_row[col] = "Standard"

                    validated_rows.append(validated_row)

                except Exception as e:
                    self.logger.warning(f"Error processing row {idx}: {e}")
                    skipped_rows += 1
                    continue

            self.logger.info(f"Validated {len(validated_rows)} rows, skipped {skipped_rows} rows")
            self.logger.info(
                f"Missing keys - Date: {missing_keys['date']}, Product: {missing_keys['product']}, "
                f"Customer: {missing_keys['customer']}, Salesperson: {missing_keys['salesperson']}"
            )

            if not validated_rows:
                self.logger.error("No valid rows to insert")
                return False

            # Get table columns from database
            self.cursor.execute(f"DESCRIBE {table_name}")
            table_columns_result = self.cursor.fetchall()
            table_columns = [col["Field"] for col in table_columns_result]
            self.logger.info(f"Database table columns: {table_columns}")

            # Filter validated rows to only include columns that exist in the table
            columns_to_insert = [
                col for col in validated_rows[0].keys() if col in table_columns
            ]

            if not columns_to_insert:
                self.logger.error("No matching columns found")
                return False

            self.logger.info(f"Columns to insert: {columns_to_insert}")

            # Prepare insert statement
            placeholders = ", ".join(["%s"] * len(columns_to_insert))
            column_str = ", ".join(columns_to_insert)
            insert_query = f"INSERT INTO {table_name} ({column_str}) VALUES ({placeholders})"

            # Insert in batches
            batch_size = 100
            total_inserted = 0

            for i in range(0, len(validated_rows), batch_size):
                batch = validated_rows[i : i + batch_size]
                batch_values = []

                for row_data in batch:
                    values = [row_data[col] for col in columns_to_insert]
                    batch_values.append(tuple(values))

                try:
                    self.cursor.executemany(insert_query, batch_values)
                    self.connection.commit()
                    total_inserted += len(batch)
                    self.logger.info(f"  Loaded batch {i//batch_size + 1} ({total_inserted} rows)")
                except Exception as e:
                    self.logger.error(f"Error inserting batch: {e}")
                    # Try one by one to identify problematic rows
                    for j, values in enumerate(batch_values):
                        try:
                            self.cursor.execute(insert_query, values)
                            self.connection.commit()
                            total_inserted += 1
                        except Exception as e2:
                            self.logger.error(f"Error inserting row {i+j}: {e2}")
                            self.logger.error(f"  Values: {values}")

            self.logger.info(f"✅ Successfully loaded {total_inserted} rows into {table_name}")

            # Final verification
            self.cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
            final_count = self.cursor.fetchone()["count"]
            self.logger.info(f"Final row count in {table_name}: {final_count}")

            return True

        except Exception as e:
            self.logger.error(f"Error loading {table_name}: {e}")
            self.connection.rollback()
            return False

    def verify_data_load(self):
        """Verify data was loaded correctly"""
        self.logger.info("Verifying data load...")

        if not self.connection or not self.cursor:
            self.logger.error("No database connection")
            return False

        try:
            tables = [
                "dim_date",
                "dim_customer",
                "dim_product",
                "dim_salesperson",
                "fact_sales",
            ]
            all_good = True
            results = {}

            for table in tables:
                self.cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                result = self.cursor.fetchone()
                count = result["count"] if result else 0
                results[table] = count

                if count > 0:
                    self.logger.info(f"  ✓ {table}: {count} rows")
                else:
                    self.logger.warning(f"  ⚠ {table}: EMPTY")
                    all_good = False

            # Additional verification for fact table
            if results.get("fact_sales", 0) > 0:
                self.cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(DISTINCT date_key) as unique_dates,
                        COUNT(DISTINCT product_key) as unique_products,
                        COUNT(DISTINCT customer_key) as unique_customers,
                        SUM(total_amount) as total_revenue,
                        SUM(profit) as total_profit
                    FROM fact_sales
                """)
                fact_stats = self.cursor.fetchone()
                self.logger.info(f"  Fact table stats:")
                self.logger.info(f"    • Unique dates: {fact_stats['unique_dates']}")
                self.logger.info(
                    f"    • Unique products: {fact_stats['unique_products']}"
                )
                self.logger.info(
                    f"    • Unique customers: {fact_stats['unique_customers']}"
                )
                self.logger.info(
                    f"    • Total revenue: ${fact_stats['total_revenue']:,.2f}"
                )
                self.logger.info(
                    f"    • Total profit: ${fact_stats['total_profit']:,.2f}"
                )

            return all_good

        except Exception as e:
            self.logger.error(f"Error verifying data: {e}")
            return False

    def close_connection(self):
        """Close database connection"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            self.logger.info("✅ Database connection closed")
        except Exception as e:
            self.logger.error(f"Error closing connection: {e}")

    def run_etl(self):
        """Run the ETL pipeline - kept for backward compatibility"""
        self.logger.info("Use ETLOrchestrator.run_etl_pipeline() instead")
        return False
