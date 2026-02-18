import mysql.connector
import pandas as pd
from config.db_config import DB_CONFIG


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def load_table(df: pd.DataFrame, table_name: str):
    """
    Generic, safe loader for dimension & fact tables.
    - Uses explicit column list
    - Skips auto-increment surrogate keys
    - Uses INSERT IGNORE for idempotency
    """

    if df.empty:
        print(f"⚠️ {table_name}: DataFrame is empty, skipping load")
        return

    conn = get_connection()
    cursor = conn.cursor()

    # 🔥 NEVER insert surrogate keys
    AUTO_KEYS = {
        "dim_date": ["date_key"],
        "dim_customer": ["customer_key"],
        "dim_product": ["product_key"],
        "fact_sales": ["sales_key"],
    }

    if table_name in AUTO_KEYS:
        df = df.drop(columns=AUTO_KEYS[table_name], errors="ignore")

    columns = df.columns.tolist()
    placeholders = ", ".join(["%s"] * len(columns))
    column_list = ", ".join(columns)

    sql = f"""
        INSERT IGNORE INTO {table_name} ({column_list})
        VALUES ({placeholders})
    """

    try:
        cursor.executemany(sql, df.values.tolist())
        conn.commit()
        print(f"✅ Loaded {cursor.rowcount} rows into {table_name}")
    except Exception as e:
        print(f"❌ Error loading {table_name}: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
