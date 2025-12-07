import sqlite3
import pandas as pd
from abc import ABC, abstractmethod
from pathlib import Path

class WriterBase(ABC):
    """
    Abstract class for my writer, this then can be typed elsewhere to assume the 
    signature and also allow for different writing methods i.e. parquet, csv, sqlite (one i'll implement). 
    """
    @abstractmethod
    def write(self, df: pd.DataFrame): 
        pass


class SQLiteWriter(WriterBase):
    """
    Concrete writer that persists data into a SQLite database. 
    """
    def __init__(self, db_path: str = 'data/nyc311.db'):
        self.db_path = db_path
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _count(conn, table: str) -> int:
        """
        Count the number of rows in table to track inserts
        """
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        (count,) = cur.fetchone()
        return count
    
    def _ensure_unique_constraint(self, conn, table):
        """Add UNIQUE constraint on unique_key if missing."""
        # SQLite doesn't allow adding UNIQUE directly; use an index.
        conn.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_unique_key
            ON {table}(unique_key);
        """)
        conn.commit()

    def write(self, df: pd.DataFrame, table: str = "requests"):
        """
        Main write method to write records into dataframe, make sure that only new records are written. 
        """
        if df.empty:
            print("Writer: No records to write.")
            return
        try: 
            with sqlite3.connect(self.db_path) as conn:

                # Create table if not exists (pandas handles schema extraction)
                df.head(0).to_sql(table, conn, if_exists="append", index=False)

                # Ensure unique index exists
                self._ensure_unique_constraint(conn, table)

                # Stage into temp table
                df.to_sql("temp_import", conn, if_exists="replace", index=False)

                before = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

                # UPSERT rows using dynamic column selection
                columns = ", ".join(df.columns)

                conn.execute(f"""
                    INSERT OR IGNORE INTO {table} ({columns})
                    SELECT {columns} FROM temp_import;
                """)

                after = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                inserted = after - before

                conn.commit()

            print(f"Writer: Inserted {inserted} rows.")
        except Exception as e:
            raise RuntimeError(f"Error writing to SQLite: {e}.")
