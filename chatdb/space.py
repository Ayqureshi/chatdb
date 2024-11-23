# space.py
from state import get_last_uploaded_table, set_last_uploaded_table

# SQL keywords dictionaries (keep existing dictionaries)
sql_keywords = {
    'show': 'SELECT', 'display': 'SELECT', 'retrieve': 'SELECT', 'get': 'SELECT',
    'from': 'FROM', 'source': 'FROM', 'table': 'FROM',
    'where': 'WHERE', 'filter by': 'WHERE', 'condition': 'WHERE',
    'order by': 'ORDER BY', 'sort by': 'ORDER BY', 'arrange by': 'ORDER BY',
    'group by': 'GROUP BY', 'organized by': 'GROUP BY', 'categorize by': 'GROUP BY',
    'limit': 'LIMIT', 'restrict to': 'LIMIT', 'top': 'LIMIT'
}

aggregation_words = {
    'average': 'AVG', 'mean': 'AVG', 'median': 'MEDIAN', 'mode': 'MODE',
    'sum': 'SUM', 'total': 'SUM', 'cumulative': 'SUM',
    'count': 'COUNT', 'number of': 'COUNT', 'frequency': 'COUNT',
    'max': 'MAX', 'maximum': 'MAX', 'highest': 'MAX', 'largest': 'MAX',
    'min': 'MIN', 'minimum': 'MIN', 'smallest': 'MIN', 'lowest': 'MIN'
}

comparison_words = {
    'greater than': '>', 'more than': '>', 'higher than': '>', 'above': '>',
    'less than': '<', 'fewer than': '<', 'lower than': '<', 'below': '<',
    'equal to': '=', 'equals': '=', 'is': '=', 'same as': '=',
    'not equal to': '!=', 'does not equal': '!=', 'different from': '!=',
    'at least': '>=', 'minimum': '>=',
    'at most': '<=', 'maximum': '<='
}

def tokenize_input(input_text):
    """Tokenize the input text while preserving meaningful phrases."""
    words = input_text.lower().split()
    tokens = []
    i = 0
    while i < len(words):
        # Check for two-word phrases first
        if i < len(words) - 1:
            two_word_phrase = f"{words[i]} {words[i+1]}"
            if two_word_phrase in sql_keywords or two_word_phrase in comparison_words:
                tokens.append(two_word_phrase)
                i += 2
                continue
        
        # Single word processing
        tokens.append(words[i])
        i += 1
    
    return tokens

def natural_language_to_sql(input_text):
    """Convert natural language query to SQL."""
    tokens = tokenize_input(input_text)
    
    # Initialize parsing state
    select_parts = []
    table_name = None
    where_conditions = []
    current_clause = None
    pending_column = None
    pending_comparison = None
    
    # Check for price-related terms
    if "price" in tokens or "cost" in tokens:
        select_parts.append("SUM(price)")
    
    i = 0
    while i < len(tokens):
        token = tokens[i]
        
        # Handle SQL keywords
        if token in sql_keywords:
            current_clause = sql_keywords[token]
            i += 1
            continue
            
        # Handle aggregation functions in SELECT clause
        if current_clause == 'SELECT':
            if token in aggregation_words:
                if i + 1 < len(tokens):
                    column = tokens[i + 1]
                    agg_func = aggregation_words[token]
                    select_parts.append(f"{agg_func}({column}) AS {agg_func.lower()}_{column}")
                    i += 2
                    continue
            else:
                select_parts.append(token)
                
        # Handle FROM clause
        elif current_clause == 'FROM':
            table_name = token
            
        # Handle WHERE clause
        elif current_clause == 'WHERE':
            if pending_column is None:
                pending_column = token
            elif token in comparison_words:
                pending_comparison = comparison_words[token]
            elif pending_column and pending_comparison:
                where_conditions.append(f"{pending_column} {pending_comparison} '{token}'")
                pending_column = None
                pending_comparison = None
                
        i += 1
    
    # Use the last uploaded table if no table is explicitly mentioned
    if not table_name:
        table_name = get_last_uploaded_table()
        if not table_name:
            raise ValueError("No table specified and no table has been uploaded.")

    # Construct the SQL query
    query_parts = []
    
    # SELECT clause
    if select_parts:
        query_parts.append("SELECT " + ", ".join(select_parts))
    else:
        query_parts.append("SELECT *")
    
    # FROM clause
    query_parts.append(f"FROM {table_name}")
    
    # WHERE clause
    if where_conditions:
        query_parts.append("WHERE " + " AND ".join(where_conditions))
    
    return " ".join(query_parts)