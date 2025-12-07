import requests

from typing import List, Dict, Optional

class NYC311Reader:
    """
    This will fetch the data from NYC API. 
    We will use requests module to fetch data from the API. 
    Added limit of records so we don't try and pull all of the data.
    URL is fixed and class attribute
    Parameters 
    ----------
    limit: int
    """
    BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
    def __init__(self, limit: int = 500):
        self.limit = limit
        self.session = requests.Session()

    def fetch(self, since: Optional[str] = None) -> List[Dict]:
        """
        Fetchs data from the NYC 311 API.         
        
        Parameters
        ----------
        since: str, optional
            ISO timestamp filter, e.g. 2024-12-01. 
            If provided, fetches only records created after this timestamp. 
        
        Returns
        -------
        list of dicts
            Raw JSON records.
        """
        params = {
            "$limit": self.limit,
            "$order": "created_date DESC"
        }

        if since:
            params["$where"] = f"created_data> '{since}"

        try: 
            response = self.session.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(f"Error fetching data from NYC 311 API: {e}")
        
        return response.json()
            
