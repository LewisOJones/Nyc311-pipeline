# tests/test_schema.py

import pytest
from src.schema import NYC311Record


def test_valid_record():
    rec = NYC311Record(
        unique_key="123",
        created_date="2025-01-01T12:00:00",
        complaint_type="Noise",
        borough="MANHATTAN",
        latitude="40.7128",
        longitude="-74.0060"
    )
    d = rec.to_dict()
    assert d["unique_key"] == "123"
    assert d["complaint_type"] == "Noise"


def test_invalid_date():
    with pytest.raises(ValueError):
        NYC311Record(
            unique_key="123",
            created_date="INVALID_DATE",
            complaint_type="Noise"
        )


def test_missing_required_field():
    with pytest.raises(ValueError):
        NYC311Record(
            unique_key="123",
            created_date=None,
            complaint_type="Noise"
        )
