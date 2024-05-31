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
    


def transform_data(df):
    # Example transformation: Select specific columns and rename them
    transformed_df = df[['userId', 'id', 'title']].rename(columns={'userId': 'User_ID', 'id': 'Post_ID', 'title': 'Title'})

    # QC Check: Ensure transformed DataFrame has expected columns
    expected_columns = {'User_ID', 'Post_ID', 'Title'}
    if not expected_columns.issubset(transformed_df.columns):
        raise ValueError("Transformation failed: Expected columns are missing")

    return transformed_df