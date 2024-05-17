import requests
import pandas as pd
from sqlalchemy import create_engine


def extract_data_from_api(api_url):
    if api_url is None:
        raise Exception (f"URL:ERROR")
    else:
        response = requests.get(api_url)
    #if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        return df
    #else:
    #    PRINT("NIL")

def transform_data(df):
    # Example transformation: Select specific columns and rename them
    transformed_df = df[['userId', 'id', 'title']].rename(columns={'userId': 'User_ID', 'id': 'Post_ID', 'title': 'Title'})

    # QC Check: Ensure transformed DataFrame has expected columns
    expected_columns = {'User_ID', 'Post_ID', 'Title'}
    if not expected_columns.issubset(transformed_df.columns):
        raise ValueError("Transformation failed: Expected columns are missing")

    return transformed_df

def load_data_to_db(df, db_url, table_name):
    from sqlalchemy import create_engine

    # Create a database engine
    engine = create_engine(db_url)

    # Load data into the specified table
    df.to_sql(table_name, engine, if_exists='append', index=False)

    # QC Check: Ensure data is loaded into the database
    loaded_df = pd.read_sql_table(table_name, engine)
    if loaded_df.empty:
        raise ValueError("Data loading failed: Table is empty")

    print(f"Data appended to {table_name} table in database.")

if __name__ == "__main__":
    api_url = "https://jsonplaceholder.typicode.com/posts"
    data = extract_data_from_api(api_url)
    transformed_data = transform_data(data)
    print("Data transformation complete.")
    # Load the transformed data to the database
    db_url = "sqlite:///mydatabase.db"
    table_name = "posts"
    load_data_to_db(transformed_data, db_url, table_name)
    print("Data loading complete.")
