import requests
import pandas as pd
from sqlalchemy import create_engine

def extract_data_from_api(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        
        # QC Check: Ensure DataFrame is not empty
        if df.empty:
            raise ValueError("Extracted data is empty")
        
        return df
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")
