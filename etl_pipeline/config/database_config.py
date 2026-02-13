"""
Simplified database configuration without SQLAlchemy issues
"""

import mysql.connector
from mysql.connector import Error
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_simple_connection():
    """Get simple MySQL connection"""
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "3306")),
            user=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASSWORD", "danish@sql12345"),
            database=os.getenv("DB_NAME", "sales_dwh"),
            autocommit=True,
        )
        print("✅ MySQL connection established")
        return connection
    except Error as e:
        print(f"❌ Connection error: {e}")
        return None


def test_simple_connection():
    """Test the simplified connection"""
    print("=" * 60)
    print("SIMPLE CONNECTION TEST")
    print("=" * 60)

    conn = get_simple_connection()
    if conn and conn.is_connected():
        cursor = conn.cursor()

        # Test basic queries
        cursor.execute("SELECT DATABASE()")
        db = cursor.fetchone()[0]
        print(f"Connected to database: {db}")

        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"Number of tables: {len(tables)}")

        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cursor.fetchone()[0]
            print(f"  • {table[0]}: {count} rows")

        cursor.close()
        conn.close()
        print("✅ All tests passed!")
    else:
        print("❌ Connection failed")

    print("=" * 60)


if __name__ == "__main__":
    test_simple_connection()
