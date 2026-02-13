"""
Create fact table by joining dimensions with sales data
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class FactTableCreator:
    """Create fact table from cleaned sales data and dimensions"""
    
    def __init__(self):
        self.fact_table = None
    
    def create_fact_table(self, sales_df, dimensions, db_connection=None):
        """
        Create fact table by joining sales data with dimension keys
        
        Args:
            sales_df: Cleaned sales DataFrame
            dimensions: Dictionary of dimension DataFrames
            db_connection: Optional database connection for salesperson lookup
        """
        logger.info("Creating fact table...")
        
        # Start with sales data
        fact_df = sales_df.copy()
        
        # Convert sale_date to date only for joining
        fact_df['sale_date_only'] = pd.to_datetime(fact_df['sale_date']).dt.date
        
        # 1. Join with date dimension
        if 'dim_date' in dimensions:
            date_dim = dimensions['dim_date'].copy()
            date_dim['full_date'] = pd.to_datetime(date_dim['full_date']).dt.date
            
            fact_df = fact_df.merge(
                date_dim[['full_date', 'date_key']],
                left_on='sale_date_only',
                right_on='full_date',
                how='left'
            )
            logger.info(f"✅ Joined with date dimension: {fact_df['date_key'].notna().sum()}/{len(fact_df)} matches")
        else:
            logger.warning("Date dimension not found, creating date_key from date")
            fact_df['date_key'] = fact_df['sale_date_only'].apply(
                lambda x: int(x.strftime('%Y%m%d')) if pd.notna(x) else None
            )
        
        # 2. Join with product dimension
        if 'dim_product' in dimensions:
            product_dim = dimensions['dim_product'].copy()
            
            # Make sure we have product_key column
            if 'product_key' not in product_dim.columns:
                product_dim['product_key'] = range(1, len(product_dim) + 1)
            
            fact_df = fact_df.merge(
                product_dim[['product_id', 'product_key']],
                on='product_id',
                how='left'
            )
            logger.info(f"✅ Joined with product dimension: {fact_df['product_key'].notna().sum()}/{len(fact_df)} matches")
        else:
            logger.warning("Product dimension not found, creating product_key from product_id")
            fact_df['product_key'] = fact_df['product_id'].apply(
                lambda x: int(x[1:]) if isinstance(x, str) and x.startswith('P') else None
            )
        
        # 3. Join with customer dimension
        if 'dim_customer' in dimensions:
            customer_dim = dimensions['dim_customer'].copy()
            
            # Make sure we have customer_key column
            if 'customer_key' not in customer_dim.columns:
                customer_dim['customer_key'] = range(1, len(customer_dim) + 1)
            
            fact_df = fact_df.merge(
                customer_dim[['customer_id', 'customer_key']],
                on='customer_id',
                how='left'
            )
            logger.info(f"✅ Joined with customer dimension: {fact_df['customer_key'].notna().sum()}/{len(fact_df)} matches")
        else:
            logger.warning("Customer dimension not found, creating customer_key from customer_id")
            fact_df['customer_key'] = fact_df['customer_id'].apply(
                lambda x: int(x[4:]) if isinstance(x, str) and x.startswith('CUST') else None
            )
        
        # 4. Add salesperson_key (simplified - assign based on region)
        # IMPORTANT: We're only adding salesperson_key, NOT employee_id
        if db_connection:
            try:
                cursor = db_connection.cursor(dictionary=True)
                cursor.execute("SELECT salesperson_key, region FROM dim_salesperson")
                salespersons = cursor.fetchall()
                cursor.close()
                
                # Create mapping of region to salesperson_key (take first salesperson in each region)
                region_to_salesperson = {}
                for sp in salespersons:
                    if sp['region'] not in region_to_salesperson:
                        region_to_salesperson[sp['region']] = sp['salesperson_key']
                
                # Map region to salesperson_key
                fact_df['salesperson_key'] = fact_df['region'].map(region_to_salesperson)
                logger.info(f"✅ Assigned salesperson keys: {fact_df['salesperson_key'].notna().sum()}/{len(fact_df)} assigned")
                
            except Exception as e:
                logger.error(f"Error fetching salespersons: {e}")
                fact_df['salesperson_key'] = 1  # Default to first salesperson
        else:
            # Simple assignment: North=1, South=2, East=3, West=4
            region_mapping = {'North': 1, 'South': 2, 'East': 3, 'West': 4}
            fact_df['salesperson_key'] = fact_df['region'].map(region_mapping).fillna(1)
            logger.info("✅ Assigned salesperson keys using default mapping")
        
        # Fill any missing salesperson_key with default value 1
        fact_df['salesperson_key'] = fact_df['salesperson_key'].fillna(1).astype(int)
        
        # 5. Calculate profit if cost price is available
        if 'dim_product' in dimensions and 'cost_price' in dimensions['dim_product'].columns:
            product_costs = dimensions['dim_product'].set_index('product_id')['cost_price']
            fact_df['cost_price'] = fact_df['product_id'].map(product_costs)
            fact_df['profit'] = fact_df['total_amount'] - (fact_df['cost_price'] * fact_df['quantity'])
            logger.info("✅ Calculated profit margins")
        else:
            fact_df['profit'] = fact_df['total_amount'] * 0.2  # Assume 20% profit
            logger.info("✅ Estimated profit (20% of total)")
        
        # 6. Select final columns for fact table - EXCLUDE employee_id
        # These columns should match your MySQL fact_sales table schema
        fact_columns = [
            'date_key', 'product_key', 'customer_key', 'salesperson_key',
            'quantity', 'unit_price', 'discount', 'total_amount', 'profit'
        ]
        
        # Add payment_method and shipping_mode if they exist in the data
        if 'payment_method' in fact_df.columns:
            fact_columns.append('payment_method')
        else:
            fact_df['payment_method'] = 'Credit Card'
            fact_columns.append('payment_method')
            
        if 'shipping_mode' in fact_df.columns:
            fact_columns.append('shipping_mode')
        else:
            fact_df['shipping_mode'] = 'Standard'
            fact_columns.append('shipping_mode')
        
        # Ensure all required columns exist
        for col in fact_columns:
            if col not in fact_df.columns:
                if col in ['discount', 'profit']:
                    fact_df[col] = 0
                elif col == 'payment_method':
                    fact_df[col] = 'Credit Card'
                elif col == 'shipping_mode':
                    fact_df[col] = 'Standard'
                elif col == 'salesperson_key':
                    fact_df[col] = 1
        
        # Create final fact table with only the columns we want
        final_fact_df = fact_df[fact_columns].copy()
        
        # Remove any rows with missing foreign keys
        initial_count = len(final_fact_df)
        final_fact_df = final_fact_df.dropna(subset=['date_key', 'product_key', 'customer_key', 'salesperson_key'])
        
        if initial_count != len(final_fact_df):
            logger.warning(f"Removed {initial_count - len(final_fact_df)} rows with missing foreign keys")
        
        # Ensure all keys are integers
        for key_col in ['date_key', 'product_key', 'customer_key', 'salesperson_key']:
            if key_col in final_fact_df.columns:
                final_fact_df[key_col] = final_fact_df[key_col].astype(int)
        
        self.fact_table = final_fact_df
        
        logger.info(f"✅ Fact table created: {len(final_fact_df)} rows")
        logger.info(f"   Columns: {list(final_fact_df.columns)}")
        logger.info(f"   Total revenue: ${final_fact_df['total_amount'].sum():,.2f}")
        logger.info(f"   Total profit: ${final_fact_df['profit'].sum():,.2f}")
        logger.info(f"   Average order value: ${final_fact_df['total_amount'].mean():,.2f}")
        
        return final_fact_df
    
    def get_fact_table(self):
        """Get the created fact table"""
        return self.fact_table
    
    def save_fact_table(self, filename='data/processed/fact_sales.csv'):
        """Save fact table to CSV file"""
        import os
        
        if self.fact_table is None:
            logger.error("No fact table to save. Run create_fact_table() first.")
            return False
        
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        self.fact_table.to_csv(filename, index=False)
        logger.info(f"✅ Fact table saved to {filename} ({len(self.fact_table)} rows)")
        return True

# Test function
def test_fact_table_creator():
    """Test fact table creator with sample data"""
    print("Testing Fact Table Creator...")
    
    # Sample data
    sample_sales = pd.DataFrame({
        'sale_date': pd.date_range('2024-01-01', periods=5, freq='D'),
        'product_id': ['P001', 'P002', 'P001', 'P003', 'P002'],
        'customer_id': ['CUST001', 'CUST002', 'CUST001', 'CUST003', 'CUST002'],
        'region': ['North', 'South', 'North', 'East', 'West'],
        'quantity': [1, 2, 1, 3, 1],
        'unit_price': [100, 50, 100, 75, 50],
        'total_amount': [100, 100, 100, 225, 50],
        'discount': [0, 0.1, 0, 0.05, 0]
    })
    
    # Sample dimensions
    date_dim = pd.DataFrame({
        'full_date': pd.date_range('2024-01-01', periods=5, freq='D').date,
        'date_key': [20240101, 20240102, 20240103, 20240104, 20240105]
    })
    
    product_dim = pd.DataFrame({
        'product_id': ['P001', 'P002', 'P003'],
        'product_key': [1, 2, 3],
        'cost_price': [70, 35, 50]
    })
    
    customer_dim = pd.DataFrame({
        'customer_id': ['CUST001', 'CUST002', 'CUST003'],
        'customer_key': [1, 2, 3]
    })
    
    salesperson_dim = pd.DataFrame({
        'salesperson_key': [1, 2, 3, 4],
        'region': ['North', 'South', 'East', 'West']
    })
    
    dimensions = {
        'dim_date': date_dim,
        'dim_product': product_dim,
        'dim_customer': customer_dim,
        'dim_salesperson': salesperson_dim
    }
    
    creator = FactTableCreator()
    fact_table = creator.create_fact_table(sample_sales, dimensions)
    
    print(f"Created fact table with {len(fact_table)} rows")
    print(f"Columns: {list(fact_table.columns)}")
    print("✅ Test completed")

if __name__ == "__main__":
    test_fact_table_creator()