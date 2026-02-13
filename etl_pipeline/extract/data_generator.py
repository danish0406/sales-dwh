"""
Sales Data Generator - Creates realistic sample data for the data warehouse
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import sys

# Add parent directory to path for imports
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
from etl_pipeline.config.database_config import get_simple_connection


class SalesDataGenerator:
    """Generate realistic sales data for testing"""

    def __init__(self, num_transactions=1000):  # CHANGED: Default to 1000
        self.num_transactions = num_transactions
        self.first_names = self._get_first_names()
        self.last_names = self._get_last_names()
        self.products = self._get_products()
        self.customers = self._get_customers()
        self.salespersons = self._get_salespersons()

    def _get_first_names(self):
        """List of common first names"""
        return [
            "James",
            "Mary",
            "John",
            "Patricia",
            "Robert",
            "Jennifer",
            "Michael",
            "Linda",
            "William",
            "Elizabeth",
            "David",
            "Barbara",
            "Richard",
            "Susan",
            "Joseph",
            "Jessica",
            "Thomas",
            "Sarah",
            "Charles",
            "Karen",
            "Christopher",
            "Nancy",
            "Daniel",
            "Lisa",
            "Matthew",
            "Margaret",
            "Anthony",
            "Betty",
            "Mark",
            "Sandra",
            "Donald",
            "Ashley",
            "Steven",
            "Dorothy",
            "Paul",
            "Kimberly",
            "Andrew",
            "Emily",
            "Joshua",
            "Donna",
            "Kenneth",
            "Michelle",
            "Kevin",
            "Carol",
            "Brian",
            "Amanda",
            "George",
            "Melissa",
            "Edward",
            "Deborah",
            "Ronald",
            "Stephanie",
            "Timothy",
            "Rebecca",
            "Jason",
            "Laura",
            "Jeffrey",
            "Helen",
            "Ryan",
            "Sharon",
            "Jacob",
            "Cynthia",
            "Gary",
            "Kathleen",
            "Nicholas",
            "Amy",
            "Eric",
            "Shirley",
            "Jonathan",
            "Angela",
            "Stephen",
            "Anna",
            "Larry",
            "Ruth",
            "Justin",
            "Brenda",
            "Scott",
            "Pamela",
            "Brandon",
            "Nicole",
        ]

    def _get_last_names(self):
        """List of common last names"""
        return [
            "Smith",
            "Johnson",
            "Williams",
            "Brown",
            "Jones",
            "Garcia",
            "Miller",
            "Davis",
            "Rodriguez",
            "Martinez",
            "Hernandez",
            "Lopez",
            "Gonzalez",
            "Wilson",
            "Anderson",
            "Thomas",
            "Taylor",
            "Moore",
            "Jackson",
            "Martin",
            "Lee",
            "Perez",
            "Thompson",
            "White",
            "Harris",
            "Sanchez",
            "Clark",
            "Ramirez",
            "Lewis",
            "Robinson",
            "Walker",
            "Young",
            "Allen",
            "King",
            "Wright",
            "Scott",
            "Torres",
            "Nguyen",
            "Hill",
            "Flores",
            "Green",
            "Adams",
            "Nelson",
            "Baker",
            "Hall",
            "Rivera",
            "Campbell",
            "Mitchell",
            "Carter",
            "Roberts",
            "Turner",
            "Phillips",
            "Evans",
            "Collins",
            "Stewart",
            "Morris",
            "Rogers",
            "Cook",
            "Morgan",
            "Bell",
            "Murphy",
            "Bailey",
            "Cooper",
            "Richardson",
            "Cox",
            "Howard",
            "Ward",
            "Brooks",
            "Peterson",
            "Gray",
            "Ramirez",
            "James",
            "Watson",
            "Reed",
            "Kelly",
            "Sanders",
            "Price",
            "Bennett",
            "Wood",
            "Barnes",
            "Ross",
            "Henderson",
        ]

    def _get_products(self):
        """Define product catalog"""
        return [
            {
                "product_id": "P001",
                "product_name": "Laptop Pro",
                "category": "Electronics",
                "base_price": 1200.00,
            },
            {
                "product_id": "P002",
                "product_name": "Wireless Mouse",
                "category": "Accessories",
                "base_price": 45.99,
            },
            {
                "product_id": "P003",
                "product_name": "Gaming Chair",
                "category": "Furniture",
                "base_price": 299.99,
            },
            {
                "product_id": "P004",
                "product_name": 'Monitor 27"',
                "category": "Electronics",
                "base_price": 399.99,
            },
            {
                "product_id": "P005",
                "product_name": "Mechanical Keyboard",
                "category": "Accessories",
                "base_price": 129.99,
            },
            {
                "product_id": "P006",
                "product_name": "Webcam HD",
                "category": "Electronics",
                "base_price": 79.99,
            },
            {
                "product_id": "P007",
                "product_name": "Desk Lamp",
                "category": "Furniture",
                "base_price": 39.99,
            },
            {
                "product_id": "P008",
                "product_name": "USB-C Hub",
                "category": "Accessories",
                "base_price": 34.99,
            },
        ]

    def _get_customers(self):
        """Generate customer data with realistic names"""
        customers = []
        segments = ["Corporate", "Consumer", "Home Office"]
        regions = ["North", "South", "East", "West"]
        cities = {
            "North": ["New York", "Boston", "Chicago"],
            "South": ["Houston", "Atlanta", "Miami"],
            "East": ["Philadelphia", "Washington DC", "Baltimore"],
            "West": ["Los Angeles", "San Francisco", "Seattle"],
        }

        # Generate 100 customers (reduced from 300)
        for i in range(1, 101):
            region = random.choice(regions)

            # Generate realistic name
            first_name = random.choice(self.first_names)
            last_name = random.choice(self.last_names)
            customer_name = f"{first_name} {last_name}"

            customer = {
                "customer_id": f"CUST{i:04d}",
                "customer_name": customer_name,  # REAL NAME instead of "Customer X"
                "email": f"{first_name.lower()}.{last_name.lower()}@example.com",
                "segment": random.choice(segments),
                "city": random.choice(cities[region]),
                "state": (
                    "NY"
                    if region == "North"
                    else (
                        "CA"
                        if region == "West"
                        else "TX" if region == "South" else "MD"
                    )
                ),
                "country": "USA",
                "region": region,
                "join_date": datetime.now() - timedelta(days=random.randint(30, 1000)),
            }
            customers.append(customer)

        return customers

    def _get_salespersons(self):
        """Get salespersons from database"""
        try:
            conn = get_simple_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT salesperson_id, salesperson_name, region FROM dim_salesperson"
            )
            salespersons = cursor.fetchall()
            cursor.close()
            conn.close()
            return salespersons
        except Exception as e:
            print(f"Error fetching salespersons: {e}")
            # Return default if database fetch fails
            return [
                {
                    "salesperson_id": "SP001",
                    "salesperson_name": "John Smith",
                    "region": "North",
                },
                {
                    "salesperson_id": "SP002",
                    "salesperson_name": "Sarah Johnson",
                    "region": "South",
                },
                {
                    "salesperson_id": "SP003",
                    "salesperson_name": "Mike Brown",
                    "region": "East",
                },
                {
                    "salesperson_id": "SP004",
                    "salesperson_name": "Lisa Davis",
                    "region": "West",
                },
            ]

    def generate_sales_transactions(self):
        """Generate sales transaction data - 1000 records"""
        print(f"Generating {self.num_transactions} sales transactions...")

        transactions = []

        # Date range: Last 1 year (reduced from 2 years)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        date_range = pd.date_range(start_date, end_date, freq="H")

        for i in range(self.num_transactions):
            # Random product
            product = random.choice(self.products)

            # Random customer
            customer = random.choice(self.customers)

            # Random salesperson from same region as customer
            region_salespersons = [
                sp for sp in self.salespersons if sp["region"] == customer["region"]
            ]
            salesperson = (
                random.choice(region_salespersons)
                if region_salespersons
                else random.choice(self.salespersons)
            )

            # Random sale date (more recent dates are more likely)
            days_ago = random.randint(0, 365)
            hours_ago = random.randint(0, 23)
            sale_date = end_date - timedelta(days=days_ago, hours=hours_ago)

            # Random quantity (1-5 units, weighted toward 1)
            quantity = np.random.choice(
                [1, 2, 3, 4, 5], p=[0.5, 0.25, 0.15, 0.07, 0.03]
            )

            # Random price variation (±15%)
            price_variation = random.uniform(0.85, 1.15)
            unit_price = round(product["base_price"] * price_variation, 2)

            # Random discount (0-25%, but only for 25% of transactions)
            if random.random() > 0.75:
                discount_pct = random.uniform(0.05, 0.25)
                discount = round(unit_price * quantity * discount_pct, 2)
            else:
                discount = 0

            # Calculate totals
            total_amount = round((unit_price * quantity) - discount, 2)

            # Payment methods
            payment_methods = ["Credit Card", "PayPal", "Bank Transfer", "Cash"]
            payment_method = random.choice(payment_methods)

            # Shipping modes
            shipping_modes = ["Standard", "Express", "Next Day"]
            shipping_mode = random.choice(shipping_modes)

            transaction = {
                "sale_id": f"SALE{100000 + i:06d}",
                "sale_date": sale_date,
                "product_id": product["product_id"],
                "product_name": product["product_name"],
                "category": product["category"],
                "customer_id": customer["customer_id"],
                "customer_name": customer["customer_name"],  # REAL NAME
                "salesperson_id": salesperson["salesperson_id"],
                "quantity": quantity,
                "unit_price": unit_price,
                "discount": discount,
                "total_amount": total_amount,
                "payment_method": payment_method,
                "shipping_mode": shipping_mode,
                "region": customer["region"],
            }

            transactions.append(transaction)

            # Progress indicator
            if (i + 1) % 200 == 0:  # Show progress every 200 records
                print(f"  Generated {i + 1} transactions...")

        # Create DataFrame
        df = pd.DataFrame(transactions)

        # Convert date to datetime
        df["sale_date"] = pd.to_datetime(df["sale_date"])

        print(f"✅ Generated {len(df)} sales transactions")
        print(
            f"   Date range: {df['sale_date'].min().date()} to {df['sale_date'].max().date()}"
        )
        print(f"   Total revenue: ${df['total_amount'].sum():,.2f}")
        print(f"   Average order value: ${df['total_amount'].mean():,.2f}")

        # Show sample with real names
        print(f"\n📊 Sample transactions with real customer names:")
        sample = df[
            ["sale_date", "customer_name", "product_name", "quantity", "total_amount"]
        ].head(3)
        for _, row in sample.iterrows():
            print(
                f"   • {row['sale_date'].strftime('%Y-%m-%d')}: {row['customer_name']} bought {row['product_name']} x{row['quantity']} for ${row['total_amount']}"
            )

        return df

    def save_to_csv(self, df, filename="sales_raw.csv"):
        """Save generated data to CSV file"""
        os.makedirs("data/raw", exist_ok=True)
        filepath = f"data/raw/{filename}"
        df.to_csv(filepath, index=False)
        print(f"✅ Data saved to {filepath}")
        return filepath

    def generate_all_data(self):
        """Generate all data and save to files"""
        print("=" * 60)
        print("GENERATING SAMPLE DATA FOR DATA WAREHOUSE")
        print("=" * 60)
        print(f"📊 Configuration:")
        print(f"   • Sales transactions: {self.num_transactions:,}")
        print(f"   • Customers: {len(self.customers):,}")
        print(f"   • Products: {len(self.products):,}")
        print("=" * 60)

        # 1. Generate sales transactions
        sales_df = self.generate_sales_transactions()
        sales_file = self.save_to_csv(sales_df, "sales_raw.csv")

        # 2. Create customer data CSV
        customer_df = pd.DataFrame(self.customers)
        customer_file = self.save_to_csv(customer_df, "customers_raw.csv")

        # 3. Create product data CSV
        product_df = pd.DataFrame(self.products)
        product_file = self.save_to_csv(product_df, "products_raw.csv")

        print("\n" + "=" * 60)
        print("DATA GENERATION COMPLETE")
        print("=" * 60)
        print(f"📁 Files created:")
        print(f"   • {sales_file} ({len(sales_df):,} rows)")
        print(f"   • {customer_file} ({len(customer_df):,} rows)")
        print(f"   • {product_file} ({len(product_df):,} rows)")

        # Show sample customer names
        print(f"\n👥 Sample customers:")
        for i, customer in enumerate(self.customers[:5]):
            print(
                f"   • {customer['customer_name']} ({customer['customer_id']}) - {customer['city']}, {customer['region']}"
            )

        return sales_df, customer_df, product_df


def main():
    """Main function to run data generator"""
    # Generate 1000 transactions with real names
    generator = SalesDataGenerator(num_transactions=1000)
    generator.generate_all_data()


if __name__ == "__main__":
    main()
