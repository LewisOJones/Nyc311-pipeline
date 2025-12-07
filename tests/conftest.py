# tests/conftest.py

import pytest
import pandas as pd
import sqlite3
from src.writer import SQLiteWriter


@pytest.fixture
def temp_db(tmp_path):
    """Creates a temporary SQLite database for isolated testing."""
    db_path = tmp_path / "test.db"
    writer = SQLiteWriter(db_path=str(db_path))
    return writer, str(db_path)
