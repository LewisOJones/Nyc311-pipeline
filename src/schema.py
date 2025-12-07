from dateutil import parser
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, Any

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

    def __init__(self, **kwargs: Any):
        self.unique_key = kwargs.get("unique_key")
        self.created_date = kwargs.get("created_date")
        self.complaint_type = kwargs.get("complaint_type")
        self.borough = kwargs.get("borough")
        self.latitude = kwargs.get("latitude")
        self.longitude = kwargs.get("longitude")

        self.__post_init__()

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

    def to_dict(self) -> dict:
        """
        Ready for pandas DataFrame
        """
        return {
            "unique_key": self.unique_key,
            "created_date": self.created_date,
            "complaint_type": self.complaint_type,
            "borough": self.borough,
            "latitude": self.latitude,
            "longitude": self.longitude
        }

    
    