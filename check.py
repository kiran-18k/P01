import requests
import pandas as pd
from sqlalchemy import create_engine


def extract_data_from_api(api_url):
    if api_url is None:
        raise Exception (f"URL:ERROR")
    else:
        response = requests.get(api_url)

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        return df
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")