import requests
import time
import os
from abc import ABC, abstractmethod
from typing import List, Dict, Optional

class ReaderBase(ABC):
    def fetch(self, *args) -> List[Dict]:
        pass


class NYC311Reader(ReaderBase):
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
    def __init__(self, limit: int = 500, app_token: Optional[str] = None):
        self.limit = limit
        self.session = requests.Session()
        self.app_token = app_token or os.getenv("NYC_APP_TOKEN")

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
            params["$where"] = f"created_date > '{since}'"

        headers = {}
        if self.app_token:
            print("Trying with App Token")
            headers['X-App-Token'] = self.app_token
        
        for attempt in range(5):
            print(f"Trying attempt {attempt + 1}...")
            wait_time = 2 ** attempt
            try: 
                response = self.session.get(
                    self.BASE_URL,
                    params=params, 
                    headers=headers,
                    timeout=10
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e: # TODO: research looks like potential app token to bypass maybe explore.  
                if response.status_code == 429:
                    print(f"Rate limit hit. Retrying in {wait_time}s.")
                    time.sleep(wait_time)
                    continue

            except requests.RequestException as e:
                print(f"Network error: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
        
        raise RuntimeError(f"Error fetching data from NYC 311 API: {e}")
            
