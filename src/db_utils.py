import sqlite3
from pathlib import Path
import pandas as pd


class DBUtils:

    def __init__(self, db_path="data/nyc311.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def table_exists(self, table="requests"):
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,)
            )
            return cur.fetchone() is not None

    def count_rows(self, table="requests"):
        if not self.table_exists(table):
            return 0
        with self._connect() as conn:
            (count,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            return count

    def preview(self, table="requests", n=5):
        if not self.table_exists(table):
            return pd.DataFrame()
        with self._connect() as conn:
            return pd.read_sql(f"SELECT * FROM {table} LIMIT {n}", conn)

    def get_latest_timestamp(self):
        if not self.table_exists("requests"):
            return None
        with self._connect() as conn:
            (ts,) = conn.execute("SELECT MAX(created_date) FROM requests").fetchone()
            return ts

    def drop_table(self, table="requests"):
        with self._connect() as conn:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
            conn.commit()
        print(f"Dropped table {table}.")


if __name__ == '__main__':
    db = DBUtils()
    print("Rows: ", db.count_rows())
    print(db.preview(n=40))