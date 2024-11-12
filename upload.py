import pandas as pd
from sqlalchemy import create_engine

def create_sql_engine(username, password, host, database):
    # MySQL connection string
    url = f"mysql+pymysql://{username}:{password}@{host}/{database}"
    return create_engine(url)

def upload_csv_to_sql(file_path, table_name, engine):
    try:
        # Load the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        # Upload the DataFrame to SQL
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print(f"CSV data uploaded to SQL table '{table_name}' successfully.")
    except Exception as e:
        print(f"An error occurred while uploading CSV: {e}")

def upload_json_to_sql(file_path, table_name, engine):
    try:
        # Load the JSON file into a DataFrame
        df = pd.read_json(file_path)
        # Upload the DataFrame to SQL
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print(f"JSON data uploaded to SQL table '{table_name}' successfully.")
    except Exception as e:
        print(f"An error occurred while uploading JSON: {e}")

# Set up the SQL engine connection with your credentials
sql_engine = create_sql_engine('root', '', 'localhost', 'dsci551')

# Example usage to upload a CSV file
upload_csv_to_sql('airtravel.csv', 'fred', sql_engine)

# # Example usage to upload a JSON file
# upload_json_to_sql('path_to_your_json_file.json', 'your_table_name', sql_engine)

