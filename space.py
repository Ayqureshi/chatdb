import spacy
import pandas as pd
from sqlalchemy import create_engine
from explore_and_index import list_tables_and_attributes  # Using the function from your file

# Load spaCy English model
nlp = spacy.load("en_core_web_sm")

# Create SQL engine using your helper function
def create_sql_engine(username, password, host, database):
    url = f"mysql+pymysql://{username}:{password}@{host}/{database}"
    return create_engine(url)

# Set up SQL engine connection
sql_engine = create_sql_engine('root', '', 'localhost', 'dsci551')

# Retrieve table and column information
tables_info = list_tables_and_attributes(sql_engine)

def parse_nl_query(nl_query, tables_info):
    """
    Parse a natural language query to extract entities and keywords for SQL query construction.
    """
    doc = nlp(nl_query)
    table_name = None
    columns = []
    conditions = []

    # Identify the table name by checking against all known tables
    for table in tables_info.keys():
        if table in nl_query.lower():
            table_name = table
            break  # Stop once we find the first matching table name

    # If table is recognized, identify columns and potential filter values
    if table_name:
        available_columns = tables_info[table_name]

        for token in doc:
            token_text = token.text.strip('"')

            # Check if token matches a column in the table
            if token_text in available_columns:
                columns.append(token_text)
            # Treat it as a filter value for "Month" if it’s not a column
            elif "Month" in available_columns:
                # Assuming any non-column token is a potential filter value for the "Month" column
                conditions.append(f"Month = '{token_text.upper()}'")

    # Default to selecting all columns if none are specified
    if not columns:
        columns = ["*"]

    return table_name, columns, conditions

def construct_sql_query(table_name, columns, conditions):
    sql_query = f"SELECT {', '.join(columns)} FROM {table_name}"
    if conditions:
        condition_str = " AND ".join(conditions)
        sql_query += f" WHERE {condition_str}"
    return sql_query

def natural_language_to_sql(nl_query):
    table_name, columns, conditions = parse_nl_query(nl_query, tables_info)
    if table_name is None:
        raise ValueError("Table name could not be determined.")
    sql_query = construct_sql_query(table_name, columns, conditions)
    return sql_query

def execute_sql_query(sql_query, engine):
    try:
        result = pd.read_sql(sql_query, con=engine)
        print("Query executed successfully. Here are the results:")
        print(result)
    except Exception as e:
        print(f"An error occurred while executing the query: {e}")

# Example usage
nl_query = "show everying in fred"
sql_query = natural_language_to_sql(nl_query)
print("Generated SQL Query:", sql_query)

# Execute and display results
execute_sql_query(sql_query, sql_engine)
