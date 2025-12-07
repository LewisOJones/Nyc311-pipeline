# alerts/notify.py

import sqlite3
import pandas as pd
from datetime import datetime, timedelta


class AlertEngine:
    """
    Generates simple alerts based on recent NYC 311 complaints.
    """

    def __init__(self, db_path="data/nyc311.db"):
        self.db_path = db_path

    def _load_df(self):
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql("SELECT * FROM requests", conn)
        df["created_date"] = pd.to_datetime(df["created_date"])
        return df

    def recent_complaints(self, complaint_filter: str, hours: int = 24):
        """
        Prints all complaints matching substring `complaint_filter`
        within the last X hours.
        """
        df = self._load_df()
        cutoff = pd.Timestamp.now() - pd.Timedelta(hours=hours)

        mask = (
            df["complaint_type"]
            .str.contains(complaint_filter, case=False, na=False)
        ) & (df["created_date"] > cutoff)

        recent = df.loc[mask]

        print(f"\nAlertEngine: Complaints containing '{complaint_filter}' "
              f"in the last {hours} hours:\n")
        print(recent[["created_date", "borough", "complaint_type"]])
        print(f"\nTotal matching complaints: {len(recent)}")

        return recent
