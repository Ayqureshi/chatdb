import pymysql
from sqlalchemy import create_engine, inspect, text
from upload import create_sql_engine  # Importing from the `upload.py` file

def list_tables_and_attributes(engine):
    """List all tables and their columns in the database."""
    inspector = inspect(engine)
    tables_info = {}
    
    # Loop through tables and retrieve columns
    for table_name in inspector.get_table_names():
        columns = inspector.get_columns(table_name)
        tables_info[table_name] = [column['name'] for column in columns]
    
    # Print the tables and their columns
    for table, columns in tables_info.items():
        print(f"Table: {table}")
        print(f"Columns: {', '.join(columns)}")
        print("-" * 40)
    
    return tables_info  # Returning info in case you want to use it in other functions

def create_index(engine, table_name, column_names):
    """Create an index on specified columns in a table if it doesn't already exist."""
    with engine.connect() as connection:
        # Form the index name and column names for SQL syntax
        index_name = f"idx_{table_name}_{'_'.join(column_names)}"
        
        # Modify the column specification if it's of type TEXT or BLOB
        columns_spec = []
        for col in column_names:
            result = connection.execute(text(
                f"SHOW COLUMNS FROM {table_name} LIKE :col"
            ), {"col": col})
            col_type = result.fetchone()[1]  # Accessing the 'Type' field by index
            
            # Check if column is TEXT/BLOB; if so, specify an index length (e.g., first 255 chars)
            if 'text' in col_type.lower() or 'blob' in col_type.lower():
                columns_spec.append(f"{col}(255)")  # Adjust length if needed
            else:
                columns_spec.append(col)
        
        # Join columns for the CREATE INDEX syntax
        columns = ', '.join(columns_spec)
        
        # Check if index exists
        result = connection.execute(text(
            f"SHOW INDEX FROM {table_name} WHERE Key_name = :index_name"
        ), {"index_name": index_name})
        
        if result.rowcount == 0:  # No index exists
            # Create the index with the specified length for TEXT/BLOB columns
            connection.execute(text(
                f"CREATE INDEX {index_name} ON {table_name} ({columns})"
            ))
            print(f"Index '{index_name}' created on {table_name}({columns})")
        else:
            print(f"Index '{index_name}' already exists on {table_name}({columns})")

def main():
    # Set up database connection
    engine = create_sql_engine('root', '', 'localhost', 'dsci551')
    
    # List tables and attributes
    print("Listing tables and columns in the database:")
    tables_info = list_tables_and_attributes(engine)
    
    # Example of creating indexes on the first column of each table
    for table, columns in tables_info.items():
        if columns:  # Check if there are columns in the table
            create_index(engine, table, [columns[0]])  # Index on the first column as an example

if __name__ == "__main__":
    main()
