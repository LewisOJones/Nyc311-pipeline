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

    def write(self, df: pd.DataFrame, table: str = "requests"):
        """
        Main write method to write records into dataframe, make sure that only new records are written. 
        """
        if df.empty:
            print("Writer: No records to write.")
            return
        try: 
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql(table, conn, if_exists="append", index=False)
            print(f"Writer: Inserted {len(df)} rows into table '{table}'.")
        except Exception as e:
            raise RuntimeError(f"Error writing to SQLite: {e}.")
