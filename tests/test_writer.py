# tests/test_writer.py

import pandas as pd
from src.writer import SQLiteWriter
import sqlite3


def test_writer_inserts_and_avoids_duplicates(temp_db):
    writer, db_path = temp_db

    df = pd.DataFrame([
        {"unique_key": "1", "created_date": "2025-01-01", "complaint_type": "Noise"},
        {"unique_key": "2", "created_date": "2025-01-02", "complaint_type": "Heat"},
    ])

    # First write should insert 2 rows
    writer.write(df, table="requests")

    # Second write with SAME df should insert 0 rows
    writer.write(df, table="requests")

    # Verify only 2 rows present
    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM requests").fetchone()[0]

    assert count == 2
