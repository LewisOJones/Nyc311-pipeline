from dateutil import parser
from datetime import datetime
from dataclasses import dataclass, field, fields
from typing import Optional, Any, Dict

@dataclass
class NYC311Record:
    unique_key: str 
    created_date: str
    complaint_type: str
    borough: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Any converted fields push here.
    created_dt: datetime = field(init=False)

    def __post_init__(self):
        if not self.created_date or not self.complaint_type:
            raise ValueError("Missing required fields.")
        
        try:
            self.created_dt = parser.parse(self.created_date)
        except: 
            raise ValueError(f"Invalid created_date: {self.created_date}")
        
        # Validate lat/lon as floats
        self.latitude = self._safe_float(self.latitude)
        self.longitude = self._safe_float(self.longitude)

    @staticmethod
    def _safe_float(val):
        try: 
            return float(val)
        except (TypeError, ValueError):
            return None

    @classmethod
    def from_api(cls, raw: Dict[str, Any]) -> "NYC311Record":
        """
        Factory for the class that takes the raw API dict (which has loads of fields) and
        filters it down to the fields this dataclass cares about.
        """
        field_names = {f.name for f in fields(cls) if f.init}
        data = {name: raw.get(name) for name in field_names}
        return cls(**data)

    def to_dict(self) -> dict:
        """
        Ready for pandas DataFrame
        """
        return {f.name: getattr(self, f.name) for f in fields(self) if f.init}

    
    