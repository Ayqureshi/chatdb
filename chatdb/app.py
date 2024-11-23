from flask import Flask, request, render_template, jsonify
import os
import mysql.connector
import pandas as pd
from pymongo import MongoClient
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta
import json
import random
from flask_cors import CORS
from space import natural_language_to_sql
from state import set_last_uploaded_table, get_last_uploaded_table


app = Flask(__name__)
CORS(app) 

# Configure file upload settings
DB_TYPE=0
UPLOAD_FOLDER = 'uploads/'
ALLOWED_EXTENSIONS = {'csv', 'json'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Function to check allowed file extensions
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# MySQL connection function
def get_db_connection():
    conn = mysql.connector.connect(
        host="localhost",
        port=3306,
        user="root",
        password="",
        database="dsci551",
        auth_plugin="mysql_native_password"
    )
    return conn

# MongoDB connection function
def get_mongo_connection():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["chatdb"]
    return db


# Route to serve the main page
@app.route('/')
def index():
    return render_template('index.html')

# Route to handle file upload
# Global variable to store the name of the last uploaded table
last_uploaded_table = None

@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'message': 'No file part'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'message': 'No selected file'}), 400

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        collection_name = filename.rsplit('.', 1)[0]
        if filename.endswith('.csv'):
            table_name = collection_name
            process_csv_file_and_load_to_db(filepath, table_name)
            set_last_uploaded_table(table_name)
        elif filename.endswith('.json'):
            process_json_file_and_load_to_mongo(filepath, collection_name)
            set_last_uploaded_table(collection_name)

        return jsonify({'message': 'File successfully uploaded', 'filename': filename}), 200
    else:
        return jsonify({'message': 'Invalid file type'}), 400


def create_table_from_csv(conn, df, table_name):
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    conn.commit()

    column_definitions = []
    for col in df.columns:
        col_type = df[col].dtype
        if col_type == 'int64':
            sql_type = 'INT'
        elif col_type == 'float64':
            sql_type = 'FLOAT'
        elif col_type == 'bool':
            sql_type = 'BOOLEAN'
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            sql_type = 'DATETIME'
        else:
            sql_type = 'VARCHAR(255)'
        column_definitions.append(f"{col} {sql_type}")

    columns = ", ".join(column_definitions)
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});")
    conn.commit()

def process_csv_file_and_load_to_db(filepath, table_name):
    df = pd.read_csv(filepath)
    conn = get_db_connection()
    create_table_from_csv(conn, df, table_name)

    columns = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    cursor = conn.cursor()
    for index, row in df.iterrows():
        try:
            values = [None if pd.isna(value) else value for value in row]
            cursor.execute(insert_query, tuple(values))
        except mysql.connector.Error as err:
            print(f"Error inserting row {index}: {err}")
            conn.rollback()
    conn.commit()
    conn.close()

def preprocess_json_data(data):
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict):
                if '$oid' in value:
                    data[key] = value['$oid']
                elif '$date' in value:
                    try:
                        data[key] = datetime.fromisoformat(value['$date'].replace("Z", "+00:00"))
                    except ValueError:
                        data[key] = value['$date']
                else:
                    preprocess_json_data(value)
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    preprocess_json_data(item)
            elif isinstance(value, str):
                try:
                    parsed_date = datetime.fromisoformat(value)
                    data[key] = parsed_date
                except ValueError:
                    pass
    elif isinstance(data, list):
        for item in data:
            preprocess_json_data(item)

def process_json_file_and_load_to_mongo(filepath, collection_name):
    db = get_mongo_connection()
    collection = db[collection_name]
    if collection_name in db.list_collection_names():
        collection.drop()

    with open(filepath, 'r') as json_file:
        data = json.load(json_file)
    preprocess_json_data(data)
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)
 
# Route to explore MySQL databases and show tables
@app.route('/api/explore', methods=['POST'])
def explore():
    db_type = request.json.get('db_type', '').lower()

    if db_type == 'mysql':
        try:
            connection = get_db_connection()  # Use your existing MySQL connection function
            cursor = connection.cursor()
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()

            table_details = {}
            for table in tables:
                table_name = table[0]
                cursor.execute(f"DESCRIBE {table_name}")
                attributes = cursor.fetchall()
                # Fetch sample data
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
                sample_data = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                # Format the sample data
                sample_data_formatted = []
                for row in sample_data:
                    formatted_row = {}
                    for col, val in zip(columns, row):
                        # Convert timedelta to string
                        if isinstance(val, timedelta):
                            formatted_row[col] = str(val)
                        else:
                            formatted_row[col] = val
                    sample_data_formatted.append(formatted_row)

                table_details[table_name] = {
                    'attributes': attributes,
                    'sample_data': sample_data_formatted
                }

            cursor.close()
            connection.close()

            return jsonify({"db_type": "mysql", "tables": table_details})

        except Exception as e:
            return jsonify({"error": str(e)})


    elif db_type == 'mongodb':
        try:
            db = get_mongo_connection()
            collection_names = db.list_collection_names()
            collection_details = {}

            for collection_name in collection_names:
                collection = db[collection_name]
                attributes = list(collection.find_one().keys()) if collection.find_one() else []
                sample_data = list(collection.find().limit(5))

                collection_details[collection_name] = {
                    'attributes': attributes,
                    'sample_data': sample_data
                }

            return jsonify({"db_type": "mongodb", "collections": collection_details})

        except Exception as e:
            return jsonify({"error": str(e)})

    else:
        return jsonify({"error": "Invalid database type specified."})

# Route to fetch sample queries
# @app.route('/api/sample_queries', methods=['GET'])
# def sample_queries():
#     queries = [
#         {"description": "Total sales by product category", "query": "SELECT product_category, SUM(transaction_qty * unit_price) FROM coffee_shop_sales GROUP BY product_category;"},
#         {"description": "Top 5 best-selling products", "query": "SELECT product_id, SUM(transaction_qty) AS total_sales FROM coffee_shop_sales GROUP BY product_id ORDER BY total_sales DESC LIMIT 5;"},
#         {"description": "Average sales per store", "query": "SELECT store_id, AVG(transaction_qty * unit_price) FROM coffee_shop_sales GROUP BY store_id;"}
#     ]
#     return jsonify({'queries': queries}), 200

@app.route('/api/nl_to_sql', methods=['POST'])
def nl_to_sql():
    data = request.json
    if not data or 'query' not in data:
        return jsonify({'error': 'Invalid request, query key missing'}), 400
    
    natural_query = data['query']
    try:
        # Convert natural language query to SQL
        sql_query = natural_language_to_sql(natural_query)
        
        # Only print and return the query
        print(f"Generated SQL Query: {sql_query}")
        return jsonify({'sql_query': sql_query}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500




# Route to handle receiving and executing MySQL queries
@app.route('/api/execute_query', methods=['POST'])
def execute_query():
    user_query = request.json.get('query')
    db_type = request.json.get('db_type', 'mysql').lower()

    if not user_query:
        return jsonify({'error': 'No query provided'}), 400
    
    if db_type == 'mysql':
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Run the query only when this endpoint is explicitly called
            cursor.execute(user_query)
            rows = cursor.fetchall()
            conn.commit()

            headers = [desc[0] for desc in cursor.description] if cursor.description else []
            result = []
            for row in rows:
                formatted_row = [str(item) if isinstance(item, timedelta) else item for item in row]
                result.append(formatted_row)

            return jsonify({'headers': headers, 'result': result}), 200
        except Exception as e:
            return jsonify({'error': str(e)}), 400
        finally:
            conn.close()
    
    elif db_type == 'mongodb':
        # MongoDB Query Handling
        db = get_mongo_connection()
        try:
            user_query = json.loads(user_query)  # Parse the input query
            collection_name = user_query.get('collection')
            query = user_query.get('query', {})
            projection = user_query.get('projection')  # Optional projection
            sort = user_query.get('sort')  # Optional sort
            limit = user_query.get('limit')  # Optional limit
            skip = user_query.get('skip')  # Optional skip
            aggregation = user_query.get('aggregation')  # Optional aggregation pipeline
            lookup = user_query.get('lookup')  # Optional lookup
            unwind = user_query.get('unwind')  # Optional unwind
            group = user_query.get('group')  # Optional group

            collection = db[collection_name]

            pipeline = aggregation if aggregation else []

            if lookup:
                pipeline.append({
                    '$lookup': {
                        'from': lookup.get('from'),
                        'localField': lookup.get('localField'),
                        'foreignField': lookup.get('foreignField'),
                        'as': lookup.get('as')
                    }
                })
            if unwind:
                pipeline.append({'$unwind': unwind})
            if group:
                pipeline.append({'$group': group})
            if query:
                pipeline.append({'$match': query})
            if projection:
                pipeline.append({'$project': projection})
            if sort:
                pipeline.append({'$sort': sort})
            if skip:
                pipeline.append({'$skip': skip})
            if limit:
                pipeline.append({'$limit': limit})

            documents = list(collection.aggregate(pipeline))

            headers = list(documents[0].keys()) if documents else []
            result = [list(doc.values()) for doc in documents]

            return jsonify({'headers': headers, 'result': result}), 200
        except Exception as e:
            return jsonify({'error': str(e)}), 400

    else:
        return jsonify({'error': 'Invalid database type specified'}), 400

    
# File to database mapping
# input_to_query_mapping = {
#     'sales': {
#         'total sales': "SELECT SUM(amount) FROM sales",
#         'count sales': "SELECT COUNT(*) FROM sales",
#         'average sales': "SELECT AVG(amount) FROM sales"
#     },
#     'orders': {
#         'all orders': "Find all orders",
#         'count orders': "Count all orders"
#     }
# }

def execute_query_helper(sql_query):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description] if cursor.description else []
        result = [list(row) for row in rows]
        return {'headers': headers, 'result': result}
    except Exception as e:
        return {'error': str(e)}
    finally:
        conn.close()


@app.route('/api/chat', methods=['POST'])
def chat():
    data = request.get_json()
    if not data or 'message' not in data:
        return jsonify({'response': 'Error: No input provided.'}), 400

    message = data['message']
    try:
        # Convert to SQL
        sql_query = natural_language_to_sql(message)

        # Check if no table is available
        if not get_last_uploaded_table():
            return jsonify({'response': 'Error: No table has been uploaded yet. Please upload a dataset first.'}), 400

        # Execute SQL
        response = natural_language_to_sql(sql_query)

        if 'error' in response:
            return jsonify({'response': f"SQL Execution Error: {response['error']}"})
        
        return jsonify({'response': response})
    except Exception as e:
        return jsonify({'response': f"Failed to process query: {str(e)}"}), 500




# Determine the appropriate response based on the input message
def determine_response(message):
    # Example logic for generating sample queries
    if 'sample' in message and 'queries' in message:
        db_type = 'mysql' if 'sql' in message else 'mongodb' if 'mongodb' in message else 'unknown'
        if db_type == 'mysql':
            sample_queries = [
                {
                    "description": "Total sales by product category",
                    "query": "SELECT product_category, SUM(sales_amount) FROM sales GROUP BY product_category;"
                },
                {
                    "description": "Count of employees by department where count exceeds 10",
                    "query": "SELECT department, COUNT(employee_id) FROM employees GROUP BY department HAVING COUNT(employee_id) > 10;"
                },
                {
                    "description": "Retrieve top 5 products by total sales",
                    "query": "SELECT product_name, SUM(sales) FROM products GROUP BY product_name ORDER BY SUM(sales) DESC LIMIT 5;"
                }
            ]
            selected_query = random.choice(sample_queries)
            return f"{selected_query['description']}\nQuery:\n{selected_query['query']}"
        elif db_type == 'mongodb':
            sample_queries = [
                {
                    "description": "Total sales by product category",
                    "query": "{\"collection\": \"sales\", \"aggregation\": [{\"$group\": {\"_id\": \"$product_category\", \"totalSales\": {\"$sum\": \"$sales_amount\"}}}]}"
                },
                {
                    "description": "Count of employees grouped by department with count > 10",
                    "query": "{\"collection\": \"employees\", \"aggregation\": [{\"$group\": {\"_id\": \"$department\", \"count\": {\"$sum\": 1}}}, {\"$match\": {\"count\": {\"$gt\": 10}}}]}"
                },
                {
                    "description": "Get top 5 products by total sales",
                    "query": "{\"collection\": \"products\", \"aggregation\": [{\"$group\": {\"_id\": \"$product_name\", \"totalSales\": {\"$sum\": \"$sales\"}}}, {\"$sort\": {\"totalSales\": -1}}, {\"$limit\": 5}]}"
                }
            ]
            selected_query = random.choice(sample_queries)
            return f"{selected_query['description']}\nQuery:\n{selected_query['query']}"
        else:
            return "Please specify SQL or MongoDB for sample queries."
    
    # Default response
    return "Unrecognized command or database reference."

    
if __name__ == '__main__':
    app.run()
