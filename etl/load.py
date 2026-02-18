import mysql.connector

def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="danish@sql12345",
        database="sales_dwh"
    )

def load_table(df, table_name):
    conn = get_connection()
    cur = conn.cursor()

    placeholders = ", ".join(["%s"] * len(df.columns))
    sql = f"""
    INSERT IGNORE INTO {table_name}
    VALUES ({placeholders})
    """

    cur.executemany(sql, df.values.tolist())
    conn.commit()

    print(f"{cur.rowcount} new rows inserted into {table_name}")

    cur.close()
    conn.close()


