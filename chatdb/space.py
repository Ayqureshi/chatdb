from state import get_last_uploaded_table

# Enhanced SQL keywords dictionary
sql_keywords = {
    'get': 'SELECT', 'show': 'SELECT', 'display': 'SELECT', 'find': 'SELECT', 'list': 'SELECT',
    'from': 'FROM', 'in': 'FROM',
    'where': 'WHERE', 'with': 'WHERE', 'which': 'WHERE', 'whose': 'WHERE', 'that': 'WHERE',
    'order by': 'ORDER BY', 'sort by': 'ORDER BY',
    'group by': 'GROUP BY', 'grouped by': 'GROUP BY', 'group': 'GROUP BY',
    'having': 'HAVING',
    'limit': 'LIMIT',
    'join': 'JOIN', 'joined with': 'JOIN', 'combined with': 'JOIN',
    'inner join': 'INNER JOIN', 'left join': 'LEFT JOIN', 'right join': 'RIGHT JOIN',
    'on': 'ON'
}

# Enhanced aggregation words
aggregation_words = {
    'average': 'AVG', 'mean': 'AVG', 'avg': 'AVG',
    'sum': 'SUM', 'total': 'SUM',
    'count': 'COUNT', 'number of': 'COUNT', 'quantity': 'COUNT',
    'max': 'MAX', 'maximum': 'MAX', 'highest': 'MAX',
    'min': 'MIN', 'minimum': 'MIN', 'lowest': 'MIN',
    'distinct': 'DISTINCT', 'unique': 'DISTINCT'
}

# Enhanced comparison words
comparison_words = {
    'is': '=', 'equals': '=', '=': '=', 'equal to': '=', 'are': '=',
    'greater than': '>', 'more than': '>', 'above': '>', 'exceeds': '>',
    'less than': '<', 'below': '<', 'under': '<',
    'not': '!=', "isn't": '!=', 'isnt': '!=', 'is not': '!=', 'does not equal': '!=',
    'at least': '>=', 'greater than or equal to': '>=', 'minimum of': '>=',
    'at most': '<=', 'less than or equal to': '<=', 'maximum of': '<='
}

# Joining words
joining_words = {
    'and', 'or', 'but', 'the', 'a', 'an', 'of', 'with', 'for', 'from',
    'by', 'in', 'on', 'at', 'to', 'sales', 'products', 'stores'
}
def tokenize_input(input_text):
    """Enhanced tokenizer to process natural language SQL input."""
    words = input_text.lower().split()
    tokens = []
    i = 0
    
    while i < len(words):
        # Ignore "all" if it's not part of a valid SQL context
        if words[i] == "all" and (i + 1 >= len(words) or words[i + 1] not in sql_keywords):
            i += 1
            continue
        
        # Try multi-word combinations (up to 4 words)
        found_match = False
        for length in range(4, 0, -1):
            if i <= len(words) - length:
                phrase = ' '.join(words[i:i+length])
                if phrase in sql_keywords or phrase in comparison_words:
                    tokens.append(phrase)
                    i += length
                    found_match = True
                    break
        
        if not found_match:
            # Handle special cases for column names with underscores
            if '_' in words[i]:
                tokens.append(words[i])
            elif words[i] not in joining_words:
                if (words[i] in sql_keywords or 
                    words[i] in aggregation_words or 
                    words[i] in comparison_words):
                    tokens.append(words[i])
                else:
                    tokens.append(words[i])
            i += 1
    
    return tokens


def natural_language_to_sql(input_text):
    """Convert natural language to SQL query."""
    tokens = tokenize_input(input_text)
    
    # Initialize query components
    select_columns = []
    table_name = get_last_uploaded_table()  # Placeholder function for table name
    where_conditions = []
    having_conditions = []
    order_by = []
    group_by = []
    joins = []
    limit = None
    
    # State tracking
    current_clause = 'SELECT'
    current_column = None
    current_comparison = None
    current_join_type = None
    current_join_table = None
    current_join_condition = None
    in_having = False
    
    i = 0
    while i < len(tokens):
        token = tokens[i].lower() if isinstance(tokens[i], str) else tokens[i]
        
        # Handle SQL keywords and clause transitions
        if token in sql_keywords:
            sql_keyword = sql_keywords[token]
            if sql_keyword in ['JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN']:
                current_clause = 'JOIN'
                current_join_type = sql_keyword
            elif sql_keyword == 'ON':
                current_clause = 'ON'
            elif sql_keyword == 'HAVING':
                current_clause = 'HAVING'
                in_having = True
            else:
                current_clause = sql_keyword
                in_having = False
            i += 1
            continue
        
        # Handle different clauses
        if current_clause == 'SELECT':
            if token in aggregation_words:
                agg_function = aggregation_words[token]
                if i + 1 < len(tokens):
                    agg_column = tokens[i + 1]
                    col_expr = f"{agg_function}({agg_column})"
                    if in_having:
                        having_conditions.append(col_expr)
                    else:
                        select_columns.append(col_expr)
                    i += 2
                    continue
            elif token not in sql_keywords and token != "select":
                # Handle table.column notation
                if '.' in token:
                    select_columns.append(token)
                else:
                    select_columns.append(token)
        
        elif current_clause == 'JOIN':
            if token not in sql_keywords:
                current_join_table = token
                joins.append({
                    'type': current_join_type or 'JOIN',
                    'table': token,
                    'condition': None
                })
        
        elif current_clause == 'ON':
            if not current_join_condition:
                current_join_condition = token
            elif token in comparison_words:
                comparison = comparison_words[token]
                if i + 1 < len(tokens):
                    joins[-1]['condition'] = f"{current_join_condition} {comparison} {tokens[i+1]}"
                    i += 2
                    current_join_condition = None
                    continue
        
        elif current_clause == 'GROUP BY':
            if token not in sql_keywords:
                group_by.append(token)
        
        elif current_clause == 'WHERE':
            if token not in sql_keywords:
                column = token
                if i + 2 < len(tokens):
                    if tokens[i + 1] in comparison_words:
                        comparison = comparison_words[tokens[i + 1]]
                        value = tokens[i + 2]
                        try:
                            float(value)
                            where_conditions.append(f"{column} {comparison} {value}")
                        except ValueError:
                            where_conditions.append(f"{column} {comparison} '{value}'")
                        i += 3
                        continue
        
        i += 1
    
    # Ensure GROUP BY columns are included in SELECT clause
    if group_by:
        for column in group_by:
            if column not in select_columns:
                select_columns.append(column)
    
    select_columns = [col for col in select_columns if col and col.strip()]
    
    # Build the SQL query
    query_parts = []

    # SELECT clause
    if select_columns:
        query_parts.append(f"SELECT {', '.join(select_columns)}")
    else:
        query_parts.append("SELECT *")
    
    # FROM clause
    if table_name:
        query_parts.append(f"FROM {table_name}")
    
    # JOIN clauses
    for join in joins:
        if join['condition']:
            query_parts.append(f"{join['type']} {join['table']} ON {join['condition']}")
        else:
            query_parts.append(f"{join['type']} {join['table']}")
    
    # WHERE clause
    if where_conditions:
        query_parts.append(f"WHERE {' AND '.join(where_conditions)}")
    
    # GROUP BY clause
    if group_by:
        query_parts.append(f"GROUP BY {', '.join(group_by)}")
    
    # HAVING clause
    if having_conditions:
        query_parts.append(f"HAVING {' AND '.join(having_conditions)}")
    
    # ORDER BY clause
    if order_by:
        query_parts.append(f"ORDER BY {', '.join(order_by)}")
    
    # LIMIT clause
    if limit:
        query_parts.append(f"LIMIT {limit}")
    
    query_parts = [part.strip(",") for part in query_parts if part.strip(",")]
    
    # Join all parts into the final SQL query
    sql_query = " ".join(query_parts).replace(",,", ",")
    
    return sql_query.strip()
