"""
Data cleaning and transformation functions for ETL pipeline
"""
import pandas as pd
import numpy as np
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataCleaner:
    """Clean and transform raw data for data warehouse"""
    
    def __init__(self):
        self.cleaned_data = {}
    
    def clean_sales_data(self, sales_df):
        """Clean sales transaction data"""
        logger.info("Cleaning sales data...")
        
        # Create a copy
        clean_df = sales_df.copy()
        
        # 1. Convert date column to datetime
        clean_df['sale_date'] = pd.to_datetime(clean_df['sale_date'], errors='coerce')
        
        # 2. Handle missing values
        clean_df['discount'] = clean_df['discount'].fillna(0)
        clean_df['payment_method'] = clean_df['payment_method'].fillna('Credit Card')
        clean_df['shipping_mode'] = clean_df['shipping_mode'].fillna('Standard')
        clean_df['region'] = clean_df['region'].fillna('Unknown')
        
        # 3. Ensure numeric columns are correct
        numeric_cols = ['quantity', 'unit_price', 'discount', 'total_amount']
        for col in numeric_cols:
            clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce')
        
        # 4. Remove invalid rows (business logic validation)
        initial_count = len(clean_df)
        
        # Remove rows with invalid data
        clean_df = clean_df[clean_df['quantity'] > 0]
        clean_df = clean_df[clean_df['unit_price'] > 0]
        clean_df = clean_df[clean_df['total_amount'] > 0]
        clean_df = clean_df[clean_df['sale_date'].notna()]
        
        removed_count = initial_count - len(clean_df)
        if removed_count > 0:
            logger.info(f"  Removed {removed_count} invalid rows")
        
        # 5. Remove duplicates
        clean_df = clean_df.drop_duplicates(subset=['sale_id'], keep='first')
        
        # 6. Add derived columns
        clean_df['sale_date_only'] = clean_df['sale_date'].dt.date
        clean_df['sale_year'] = clean_df['sale_date'].dt.year
        clean_df['sale_month'] = clean_df['sale_date'].dt.month
        clean_df['sale_day'] = clean_df['sale_date'].dt.day
        
        logger.info(f"✅ Cleaned {len(clean_df)} sales records")
        
        # Store cleaned data
        self.cleaned_data['sales'] = clean_df
        
        return clean_df
    
    def clean_customer_data(self, customer_df):
        """Clean customer dimension data"""
        logger.info("Cleaning customer data...")
        
        clean_df = customer_df.copy()
        
        # Standardize email format
        clean_df['email'] = clean_df['email'].str.lower().str.strip()
        
        # Fill missing values with defaults
        clean_df['segment'] = clean_df['segment'].fillna('Consumer')
        clean_df['region'] = clean_df['region'].fillna('Unknown')
        clean_df['city'] = clean_df['city'].fillna('Unknown')
        clean_df['state'] = clean_df['state'].fillna('Unknown')
        
        # Ensure join_date is datetime
        clean_df['join_date'] = pd.to_datetime(clean_df['join_date'], errors='coerce')
        clean_df['join_date'] = clean_df['join_date'].fillna(pd.Timestamp('2023-01-01'))
        
        # Ensure unique customer IDs
        clean_df = clean_df.drop_duplicates(subset=['customer_id'], keep='first')
        
        logger.info(f"✅ Cleaned {len(clean_df)} customer records")
        
        # Store cleaned data
        self.cleaned_data['customers'] = clean_df
        
        return clean_df
    
    def clean_product_data(self, product_df):
        """Clean product dimension data"""
        logger.info("Cleaning product data...")
        
        clean_df = product_df.copy()
        
        # Fill missing values
        clean_df['category'] = clean_df['category'].fillna('General')
        clean_df['sub_category'] = clean_df.get('sub_category', 'General')
        clean_df['brand'] = clean_df.get('brand', 'Default Brand')
        
        # Ensure numeric columns
        clean_df['base_price'] = pd.to_numeric(clean_df.get('base_price', 0), errors='coerce')
        clean_df['base_price'] = clean_df['base_price'].fillna(100)
        
        # Add additional columns if missing
        if 'cost_price' not in clean_df.columns:
            clean_df['cost_price'] = clean_df['base_price'] * 0.7  # Assume 30% margin
        
        if 'selling_price' not in clean_df.columns:
            clean_df['selling_price'] = clean_df['base_price']
        
        if 'supplier' not in clean_df.columns:
            clean_df['supplier'] = 'Default Supplier'
        
        if 'is_active' not in clean_df.columns:
            clean_df['is_active'] = True
        
        # Ensure unique product IDs
        clean_df = clean_df.drop_duplicates(subset=['product_id'], keep='first')
        
        logger.info(f"✅ Cleaned {len(clean_df)} product records")
        
        # Store cleaned data
        self.cleaned_data['products'] = clean_df
        
        return clean_df
    
    def validate_data_quality(self):
        """Run data quality checks on cleaned data"""
        logger.info("Running data quality checks...")
        
        issues = []
        
        if 'sales' in self.cleaned_data:
            sales_df = self.cleaned_data['sales']
            
            # Check 1: Negative amounts
            negative_amounts = sales_df[sales_df['total_amount'] < 0]
            if len(negative_amounts) > 0:
                issues.append(f"Found {len(negative_amounts)} sales with negative amounts")
            
            # Check 2: Future dates
            future_dates = sales_df[sales_df['sale_date'] > pd.Timestamp.now()]
            if len(future_dates) > 0:
                issues.append(f"Found {len(future_dates)} sales with future dates")
            
            # Check 3: Missing critical fields
            missing_customer = sales_df[sales_df['customer_id'].isna()]
            if len(missing_customer) > 0:
                issues.append(f"Found {len(missing_customer)} sales missing customer_id")
        
        if 'customers' in self.cleaned_data:
            customers_df = self.cleaned_data['customers']
            
            # Check: Invalid emails
            invalid_emails = customers_df[~customers_df['email'].str.contains('@', na=False)]
            if len(invalid_emails) > 0:
                issues.append(f"Found {len(invalid_emails)} customers with invalid email format")
        
        if issues:
            logger.warning(f"⚠️  Data quality issues found: {len(issues)}")
            for issue in issues:
                logger.warning(f"  - {issue}")
        else:
            logger.info("✅ All data quality checks passed")
        
        return len(issues) == 0
    
    def get_cleaned_data(self):
        """Get all cleaned data"""
        return self.cleaned_data
    
    def save_cleaned_data(self, output_dir='data/processed'):
        """Save cleaned data to CSV files"""
        import os
        
        os.makedirs(output_dir, exist_ok=True)
        
        for data_type, df in self.cleaned_data.items():
            filename = f"{output_dir}/{data_type}_cleaned.csv"
            df.to_csv(filename, index=False)
            logger.info(f"✅ Saved cleaned {data_type} data to {filename}")
        
        return True

# Test function
def test_data_cleaner():
    """Test the data cleaner with sample data"""
    print("Testing Data Cleaner...")
    
    # Create sample data
    sample_sales = pd.DataFrame({
        'sale_id': ['SALE001', 'SALE002', 'SALE003'],
        'sale_date': ['2024-01-15', 'invalid', '2024-01-16'],
        'product_id': ['P001', 'P002', 'P003'],
        'customer_id': ['CUST001', 'CUST002', 'CUST003'],
        'quantity': [1, 2, -1],  # Invalid: negative quantity
        'unit_price': [100, 50, 75],
        'total_amount': [100, 100, -75]  # Invalid: negative amount
    })
    
    cleaner = DataCleaner()
    cleaned_sales = cleaner.clean_sales_data(sample_sales)
    
    print(f"Original: {len(sample_sales)} rows")
    print(f"Cleaned: {len(cleaned_sales)} rows")
    print("✅ Test completed")

if __name__ == "__main__":
    test_data_cleaner()