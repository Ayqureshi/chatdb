# state.py
_last_uploaded_table = None  # Using underscore to indicate "private" variable

def set_last_uploaded_table(table_name):
    global _last_uploaded_table
    _last_uploaded_table = table_name

def get_last_uploaded_table():
    global _last_uploaded_table
    return _last_uploaded_table