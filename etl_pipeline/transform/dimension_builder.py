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
        
        if 'sale_date' not in sales_df.columns:
            logger.error("Sales data must have 'sale_date' column")
            return None
        
        # Extract unique dates from sales data
        sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
        unique_dates = sales_df['sale_date'].dt.date.unique()
        
        # Create date range (ensure we have all dates, not just sales dates)
        min_date = min(unique_dates)
        max_date = max(unique_dates)
        
        # Create full date range
        date_range = pd.date_range(start=min_date, end=max_date, freq='D')
        
        date_dim = pd.DataFrame({
            'full_date': date_range,
            'date_key': date_range.strftime('%Y%m%d').astype(int),
            'year': date_range.year,
            'quarter': date_range.quarter,
            'month': date_range.month,
            'month_name': date_range.strftime('%B'),
            'day': date_range.day,
            'day_of_week': date_range.strftime('%A'),
            'day_of_week_num': date_range.dayofweek + 1,  # Monday=1, Sunday=7
            'is_weekend': date_range.dayofweek.isin([5, 6]),
            'is_holiday': False,  # Simplified - could add holiday calendar
            'week_of_year': date_range.isocalendar().week
        })
        
        # Sort by date
        date_dim = date_dim.sort_values('full_date').reset_index(drop=True)
        
        logger.info(f"✅ Date dimension created: {len(date_dim)} dates ({min_date} to {max_date})")
        
        self.dimensions['dim_date'] = date_dim
        return date_dim
    
    def build_customer_dimension(self, customer_df):
        """Create customer dimension table"""
        logger.info("Building customer dimension...")
        
        # Required columns
        required_cols = ['customer_id', 'customer_name', 'email', 'segment', 'city', 'state', 'country', 'region']
        
        # Check if we have required columns
        missing_cols = [col for col in required_cols if col not in customer_df.columns]
        if missing_cols:
            logger.warning(f"Missing columns in customer data: {missing_cols}")
        
        # Create customer dimension
        customer_dim = customer_df.copy()
        
        # Add missing columns with defaults
        if 'segment' not in customer_dim.columns:
            customer_dim['segment'] = 'Consumer'
        
        if 'region' not in customer_dim.columns:
            customer_dim['region'] = 'Unknown'
        
        if 'loyalty_tier' not in customer_dim.columns:
            customer_dim['loyalty_tier'] = 'Standard'
        
        # Add surrogate key (auto-increment will be handled by database)
        # For pandas, we'll create a sequential key
        customer_dim = customer_dim.reset_index(drop=True)
        
        logger.info(f"✅ Customer dimension created: {len(customer_dim)} customers")
        
        self.dimensions['dim_customer'] = customer_dim
        return customer_dim
    
    def build_product_dimension(self, product_df):
        """Create product dimension table"""
        logger.info("Building product dimension...")
        
        # Create product dimension
        product_dim = product_df.copy()
        
        # Ensure required columns
        required_mapping = {
            'product_name': 'Unnamed Product',
            'category': 'General',
            'sub_category': 'General',
            'brand': 'Default Brand',
            'cost_price': 0,
            'selling_price': 0,
            'supplier': 'Default Supplier',
            'is_active': True
        }
        
        for col, default_value in required_mapping.items():
            if col not in product_dim.columns:
                if col in ['cost_price', 'selling_price']:
                    # For prices, use base_price if available
                    if 'base_price' in product_dim.columns:
                        if col == 'cost_price':
                            product_dim[col] = product_dim['base_price'] * 0.7
                        else:
                            product_dim[col] = product_dim['base_price']
                    else:
                        product_dim[col] = default_value
                else:
                    product_dim[col] = default_value
        
        # Calculate profit margin if we have both prices
        if 'cost_price' in product_dim.columns and 'selling_price' in product_dim.columns:
            product_dim['profit_margin'] = (
                (product_dim['selling_price'] - product_dim['cost_price']) / 
                product_dim['selling_price'] * 100
            ).round(2)
        else:
            product_dim['profit_margin'] = 30.0  # Default
        
        logger.info(f"✅ Product dimension created: {len(product_dim)} products")
        
        self.dimensions['dim_product'] = product_dim
        return product_dim
    
    def get_existing_salespersons(self, db_connection):
        """Get existing salespersons from database"""
        logger.info("Fetching existing salespersons from database...")
        
        try:
            cursor = db_connection.cursor(dictionary=True)
            cursor.execute("SELECT salesperson_id, salesperson_name, region FROM dim_salesperson")
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
            {'salesperson_id': 'SP001', 'salesperson_name': 'John Smith', 'region': 'North'},
            {'salesperson_id': 'SP002', 'salesperson_name': 'Sarah Johnson', 'region': 'South'},
            {'salesperson_id': 'SP003', 'salesperson_name': 'Mike Brown', 'region': 'East'},
            {'salesperson_id': 'SP004', 'salesperson_name': 'Lisa Davis', 'region': 'West'},
        ]
        
        salesperson_df = pd.DataFrame(default_salespersons)
        
        # Add additional columns
        salesperson_df['email'] = salesperson_df['salesperson_name'].str.replace(' ', '.').str.lower() + '@company.com'
        salesperson_df['territory'] = salesperson_df['region']
        salesperson_df['manager_id'] = 'MGR001'
        salesperson_df['hire_date'] = pd.Timestamp('2022-01-01')
        salesperson_df['commission_rate'] = 0.10
        
        logger.info("✅ Created default salesperson dimension")
        
        self.dimensions['dim_salesperson'] = salesperson_df
        return salesperson_df
    
    def get_all_dimensions(self):
        """Get all built dimension tables"""
        return self.dimensions
    
    def save_dimensions(self, output_dir='data/processed'):
        """Save dimension tables to CSV files"""
        import os
        
        os.makedirs(output_dir, exist_ok=True)
        
        for dim_name, df in self.dimensions.items():
            filename = f"{output_dir}/{dim_name}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"✅ Saved {dim_name} to {filename}")
        
        return True

# Test function
def test_dimension_builder():
    """Test dimension builder with sample data"""
    print("Testing Dimension Builder...")
    
    # Sample data
    sample_sales = pd.DataFrame({
        'sale_date': pd.date_range('2024-01-01', periods=10, freq='D'),
        'product_id': ['P001'] * 10,
        'customer_id': ['CUST001'] * 10
    })
    
    sample_customers = pd.DataFrame({
        'customer_id': ['CUST001', 'CUST002'],
        'customer_name': ['John Doe', 'Jane Smith'],
        'email': ['john@example.com', 'jane@example.com']
    })
    
    sample_products = pd.DataFrame({
        'product_id': ['P001', 'P002'],
        'product_name': ['Laptop', 'Mouse'],
        'base_price': [1000, 50]
    })
    
    builder = DimensionBuilder()
    
    # Build dimensions
    date_dim = builder.build_date_dimension(sample_sales)
    customer_dim = builder.build_customer_dimension(sample_customers)
    product_dim = builder.build_product_dimension(sample_products)
    
    print(f"Date dimension: {len(date_dim)} rows")
    print(f"Customer dimension: {len(customer_dim)} rows")
    print(f"Product dimension: {len(product_dim)} rows")
    print("✅ Test completed")

if __name__ == "__main__":
    test_dimension_builder()