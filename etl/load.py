import mysql.connector

def get_conn():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        database="sales_dwh"
    )

def load_table(df, table):
    conn = get_conn()
    cur = conn.cursor()

    placeholders = ",".join(["%s"] * len(df.columns))
    cols = ",".join(df.columns)

    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

    cur.executemany(sql, df.values.tolist())
    conn.commit()

    cur.close()
    conn.close()
