import requests
import pandas as pd
from sqlalchemy import create_engine


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


def load_data_to_db(df, db_url, table_name):
    # Create a database engine
    engine = create_engine(db_url)
    
    # Load data into the specified table
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    
    # QC Check: Ensure data is loaded into the database
    loaded_df = pd.read_sql_table(table_name, engine)
    if loaded_df.empty:
        raise ValueError("Data loading failed: Table is empty")
    
    print(f"Data loaded to {table_name} table in database.")

if __name__ == "__main__":
    api_url = "https://jsonplaceholder.typicode.com/posts"  
    data = extract_data_from_api(api_url)
    #data.to_excel('extracted_data.xlsx', index=False)
    print("Data extraction complete. Saved to extracted_data.csv")



