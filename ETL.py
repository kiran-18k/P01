import requests
import pandas as pd

def extract_data_from_api(api_url):
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

if __name__ == "__main__":
    api_url = "https://jsonplaceholder.typicode.com/posts"  # Sample API for demonstration
    data = extract_data_from_api(api_url)
    data.to_csv('extracted_data.csv', index=False)
    print("Data extraction complete. Saved to extracted_data.csv")



