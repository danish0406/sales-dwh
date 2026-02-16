"""
MySQL Loader Module
Handles loading dimension and fact tables into MySQL
"""

import logging
import pandas as pd
from mysql.connector import Error

logger = logging.getLogger(__name__)


class MySQLLoader:
    def __init__(self):
        self.connection = None
        self.cursor = None
        self._connect()

    def _connect(self):
        try:
            from etl_pipeline.config.database_config import get_simple_connection
            self.connection = get_simple_connection()
            self.cursor = self.connection.cursor(dictionary=True)
            logger.info("✅ MySQL connection established")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise

    # ------------------------------------------------------------------
    # TABLE CLEANUP
    # ------------------------------------------------------------------
    def clear_all_tables_safely(self):
        logger.info("Clearing all tables safely...")

        try:
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

            clear_order = [
                "fact_sales",
                "dim_date",
                "dim_customer",
                "dim_product",
                "dim_salesperson",
            ]

            for table in clear_order:
                self.cursor.execute(f"TRUNCATE TABLE {table}")
                logger.info(f"  ✓ Truncated {table}")

            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            self.connection.commit()

            logger.info("✅ All tables cleared")
            return True

        except Exception as e:
            self.connection.rollback()
            logger.error(f"❌ Failed clearing tables: {e}")
            return False

    # ------------------------------------------------------------------
    # DIMENSION LOADER
    # ------------------------------------------------------------------
    def load_dimension_table(self, df: pd.DataFrame, table_name: str):
        logger.info(f"Loading {table_name} ({len(df)} rows)")

        try:
            self.cursor.execute(f"DESCRIBE {table_name}")
            table_cols = [row["Field"] for row in self.cursor.fetchall()]

            df = df[[c for c in df.columns if c in table_cols]].copy()

            df = self._force_python_types(df)

            insert_sql = f"""
                INSERT INTO {table_name} ({", ".join(df.columns)})
                VALUES ({", ".join(["%s"] * len(df.columns))})
            """

            values = [tuple(row) for row in df.itertuples(index=False, name=None)]

            self.cursor.executemany(insert_sql, values)
            self.connection.commit()

            logger.info(f"✅ Loaded {len(values)} rows into {table_name}")
            return True

        except Exception as e:
            self.connection.rollback()
            logger.error(f"❌ Failed loading {table_name}: {e}")
            return False

    # ------------------------------------------------------------------
    # FACT TABLE LOADER (FIXED)
    # ------------------------------------------------------------------
    def load_fact_table(self, df: pd.DataFrame, table_name: str):
        logger.info(f"Loading {table_name} ({len(df)} rows)")

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

        df = df[[c for c in expected_columns if c in df.columns]].copy()

        # 🔴 CRITICAL FIX: eliminate numpy / pandas dtypes
        df = self._force_python_types(df)

        try:
            insert_sql = f"""
                INSERT INTO {table_name} ({", ".join(df.columns)})
                VALUES ({", ".join(["%s"] * len(df.columns))})
            """

            batch_size = 100
            total = 0

            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                values = [tuple(row) for row in batch.itertuples(index=False, name=None)]
                self.cursor.executemany(insert_sql, values)
                self.connection.commit()
                total += len(values)
                logger.info(f"  ✓ Loaded {total} rows")

            logger.info(f"✅ Successfully loaded {total} rows into {table_name}")
            return True

        except Exception as e:
            self.connection.rollback()
            logger.error(f"❌ Failed loading fact table: {e}")
            return False

    # ------------------------------------------------------------------
    # TYPE SANITIZATION (MOST IMPORTANT FUNCTION)
    # ------------------------------------------------------------------
    def _force_python_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert ALL values to pure Python types.
        This prevents numpy / pandas dtype crashes.
        """

        for col in df.columns:
            if pd.api.types.is_integer_dtype(df[col]):
                df[col] = df[col].astype("int64").map(int)
            elif pd.api.types.is_float_dtype(df[col]):
                df[col] = df[col].astype("float64").map(float)
            else:
                df[col] = df[col].astype(str)

        return df

    # ------------------------------------------------------------------
    # VERIFICATION
    # ------------------------------------------------------------------
    def verify_data_load(self):
        tables = [
            "dim_date",
            "dim_customer",
            "dim_product",
            "dim_salesperson",
            "fact_sales",
        ]

        try:
            for table in tables:
                self.cursor.execute(f"SELECT COUNT(*) AS cnt FROM {table}")
                cnt = self.cursor.fetchone()["cnt"]
                logger.info(f"{table}: {cnt} rows")

            return True

        except Exception as e:
            logger.error(f"❌ Verification failed: {e}")
            return False

    # ------------------------------------------------------------------
    # CLEANUP
    # ------------------------------------------------------------------
    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("✅ MySQL connection closed")
