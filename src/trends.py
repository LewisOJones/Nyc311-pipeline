import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

class TrendAnalyser:
    """
    Provides simple historical trend analysis for NYC 311 service requests.
    Reads from SQLite and produces PNG plots for easy demo/display.
    """
    def __init__(self, db_path="data/nyc311.db", output_dir="analysis/output"):
        self.db_path = db_path
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def _load_df(self):
        with sqlite3.connect(self.db_path) as conn:
            try: 
                df = pd.read_sql("SELECT * FROM requests", conn)
            except pd.errors.DatabaseError as e:
                print(f"Please first load data into the database, before running analysis")
                raise

        df["created_date"] = pd.to_datetime(df["created_date"])
        return df

    def daily_volume(self):
        """
        Aggregates total complaints per day.
        """
        df = self._load_df()
        df["day"] = df["created_date"].dt.date
        counts = df.groupby("day").size()

        plt.figure(figsize=(10, 4))
        counts.plot(title="Daily Complaint Volume")
        plt.tight_layout()

        out = f"{self.output_dir}/daily_volume.png"
        plt.savefig(out)
        plt.close()

        print(f"TrendAnalyser: Saved daily volume plot → {out}")

    def top_complaints(self, n=10):
        """
        Shows top-N complaint types.
        """
        df = self._load_df()
        counts = df["complaint_type"].value_counts().head(n)

        plt.figure(figsize=(10, 4))
        counts.plot(kind="bar", title=f"Top {n} Complaint Types")
        plt.tight_layout()

        out = f"{self.output_dir}/top_complaints.png"
        plt.savefig(out)
        plt.close()

        print(f"TrendAnalyser: Saved top complaints plot → {out}")

    def borough_distribution(self):
        """
        Visual summary of complaints by borough.
        """
        df = self._load_df()
        counts = df["borough"].value_counts()

        plt.figure(figsize=(6, 6))
        counts.plot(kind="pie", autopct="%1.1f%%", title="Complaint Distribution by Borough")
        plt.ylabel("")
        plt.tight_layout()

        out = f"{self.output_dir}/borough_distribution.png"
        plt.savefig(out)
        plt.close()

        print(f"TrendAnalyser: Saved borough distribution plot → {out}")