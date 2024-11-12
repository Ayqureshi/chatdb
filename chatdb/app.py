from flask import Flask, request, render_template, jsonify
import os
import mysql.connector
import pandas as pd
from pymongo import MongoClient
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta
import pymysql
import pymongo
import json
import re
from flask_cors import CORS


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
        host="127.0.0.1",
        port=3306,
        user="root",
        password="Dsci-551",
        database="chatdb",
        auth_plugin="mysql_native_password"
    )
    return conn

# MongoDB connection function
def get_mongo_connection():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["chatdb"]
    return db

# Test MySQL connection
def test_db_connection():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        print("MySQL connection successful")
        conn.close()
    except mysql.connector.Error as err:
        print(f"Error: {err}")

# Test MongoDB connection
def test_mongo_connection():
    try:
        db = get_mongo_connection()
        db.list_collection_names()  # Test connection by listing collections
        print("MongoDB connection successful")
    except Exception as err:
        print(f"MongoDB connection error: {err}")

# Route to serve the main page
@app.route('/')
def index():
    return render_template('index.html')

# Route to handle file upload
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

        # Set collection name based on filename (e.g., "orders.json" => "orders")
        collection_name = filename.rsplit('.', 1)[0]

        # Process file based on extension
        if filename.endswith('.csv'):
            DB_TYPE='mysql'
            test_db_connection()
            table_name = collection_name  # Use the filename without extension as table name
            process_csv_file_and_load_to_db(filepath, table_name)
        elif filename.endswith('.json'):
            DB_TYPE='mongodb'
            test_mongo_connection()
            process_json_file_and_load_to_mongo(filepath, collection_name)

        return jsonify({'message': 'File successfully uploaded', 'filename': filename}), 200
    else:
        return jsonify({'message': 'Invalid file type'}), 400

# Process CSV file and load into MySQL
def create_table_from_csv(conn, df, table_name):
    cursor = conn.cursor()

    # Drop the table if it exists
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    conn.commit()

    columns = ", ".join(f"{col} VARCHAR(255)" for col in df.columns)
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});")
    conn.commit()

def process_csv_file_and_load_to_db(filepath, table_name):
    df = pd.read_csv(filepath)
    conn = get_db_connection()
    create_table_from_csv(conn, df, table_name)

    # Dynamically generate SQL statement for insertion
    columns = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    cursor = conn.cursor()
    for index, row in df.iterrows():
        try:
            cursor.execute(insert_query, tuple(row))
        except mysql.connector.Error as err:
            print(f"Error inserting row {index}: {err}")
            conn.rollback()
    conn.commit()
    conn.close()

# Function to preprocess JSON data
def preprocess_json_data(data):
    if isinstance(data, dict):
        # Recursively preprocess each field in the dictionary
        for key, value in data.items():
            if isinstance(value, dict):
                if '$oid' in value:
                    # Replace $oid with the ObjectId string
                    data[key] = value['$oid']
                elif '$date' in value:
                    # Replace $date with a datetime object
                    data[key] = datetime.fromisoformat(value['$date'].replace("Z", "+00:00"))
                else:
                    # Recurse into nested dictionaries
                    preprocess_json_data(value)
            elif isinstance(value, list):
                # Preprocess each item in the list
                for item in value:
                    preprocess_json_data(item)
    elif isinstance(data, list):
        # If the root element is a list, preprocess each item
        for item in data:
            preprocess_json_data(item)

# Function to load JSON file to MongoDB
def process_json_file_and_load_to_mongo(filepath, collection_name):
    db = get_mongo_connection()  # Connect to MongoDB
    collection = db[collection_name]  # Use dynamic collection name based on filename

    # Drop the collection if it exists
    if collection_name in db.list_collection_names():
        collection.drop()
        print(f"Dropped existing collection: {collection_name}")

    # Load JSON data from file
    with open(filepath, 'r') as json_file:
        data = json.load(json_file)
    
    # Preprocess data to handle $oid and $date fields
    preprocess_json_data(data)
    
    # Insert data into MongoDB
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)
    print(f"MongoDB data insertion complete into {collection_name} collection.")

# Route to explore MySQL databases and show tables
@app.route('/api/explore', methods=['POST'])
def explore():
    db_type = request.json.get('db_type', '').lower()

    if db_type == 'mysql':
        try:
            connection = get_db_connection()  # Use your existing MySQL connection function
            cursor = connection.cursor()

            # Fetch tables
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
@app.route('/api/sample_queries', methods=['GET'])
def sample_queries():
    queries = [
        {"description": "Total sales by product category", "query": "SELECT product_category, SUM(transaction_qty * unit_price) FROM coffee_shop_sales GROUP BY product_category;"},
        {"description": "Top 5 best-selling products", "query": "SELECT product_id, SUM(transaction_qty) AS total_sales FROM coffee_shop_sales GROUP BY product_id ORDER BY total_sales DESC LIMIT 5;"},
        {"description": "Average sales per store", "query": "SELECT store_id, AVG(transaction_qty * unit_price) FROM coffee_shop_sales GROUP BY store_id;"}
    ]
    return jsonify({'queries': queries}), 200

# Route to handle receiving and executing MySQL queries
@app.route('/api/execute_query', methods=['POST'])
def execute_query():
    print('hey')
    user_query = request.json.get('query')
    if not user_query:
        return jsonify({'error': 'No query provided'}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(user_query)
        rows = cursor.fetchall()
        conn.commit()

        result = []
        for row in rows:
            formatted_row = [str(item) if isinstance(item, timedelta) else item for item in row]
            result.append(formatted_row)

        return jsonify({'result': result}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally:
        conn.close()
        
# File to database mapping
input_to_query_mapping = {
    'sales': {
        'total sales': "SELECT SUM(amount) FROM sales",
        'count sales': "SELECT COUNT(*) FROM sales",
        'average sales': "SELECT AVG(amount) FROM sales"
    },
    'orders': {
        'all orders': "Find all orders",
        'count orders': "Count all orders"
    }
}

# Determine the appropriate response based on the input message
def determine_response(message):
    for key in input_to_query_mapping:
        if re.search(rf'\b{key}\b', message):  # Match whole words only
            queries = input_to_query_mapping[key]
            for query_key in queries:
                if re.search(rf'\b{query_key}\b', message):
                    return queries[query_key]
    return "Unrecognized command or database reference."

@app.route('/api/chat', methods=['POST'])
def chat():
    data = request.get_json()
    if not data or 'message' not in data:
        return jsonify({'error': 'Invalid request, message key missing'}), 400
    
    message = data['message'].lower()
    response = determine_response(message)

    return jsonify({'response': response})


if __name__ == '__main__':
    app.run()
