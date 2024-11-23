# space.py
from state import get_last_uploaded_table

# Expanded SQL keywords dictionary
sql_keywords = {
    'get': 'SELECT', 'show': 'SELECT', 'display': 'SELECT', 'find': 'SELECT', 'list': 'SELECT',
    'from': 'FROM', 'in': 'FROM',
    'where': 'WHERE', 'with': 'WHERE', 'which': 'WHERE', 'whose': 'WHERE', 'that': 'WHERE',
    'order by': 'ORDER BY', 'sort by': 'ORDER BY',
    'group by': 'GROUP BY', 'grouped by': 'GROUP BY',
    'having': 'HAVING',
    'limit': 'LIMIT',
    'join': 'JOIN', 'joined with': 'JOIN', 'combined with': 'JOIN',
    'inner join': 'INNER JOIN', 'left join': 'LEFT JOIN', 'right join': 'RIGHT JOIN',
    'on': 'ON'
}

aggregation_words = {
    'average': 'AVG', 'mean': 'AVG', 'avg': 'AVG',
    'sum': 'SUM', 'total': 'SUM',
    'count': 'COUNT', 'number of': 'COUNT',
    'max': 'MAX', 'maximum': 'MAX', 'highest': 'MAX',
    'min': 'MIN', 'minimum': 'MIN', 'lowest': 'MIN'
}

comparison_words = {
    'is': '=', 'equals': '=', '=': '=', 'equal to': '=', 'are': '=',
    'greater than': '>', 'more than': '>', 'above': '>', 'exceeds': '>',
    'less than': '<', 'below': '<', 'under': '<',
    'not': '!=', "isn't": '!=', 'isnt': '!=', 'is not': '!=', 'does not equal': '!=',
    'at least': '>=', 'greater than or equal to': '>=', 'minimum of': '>=',
    'at most': '<=', 'less than or equal to': '<=', 'maximum of': '<='
}

joining_words = {'and', 'or', 'but', 'the', 'a', 'an', 'of', 'with', 'for', 'from'}

def tokenize_input(input_text):
    """Enhanced tokenizer that better handles complex SQL phrases."""
    words = input_text.split()
    tokens = []
    i = 0
    
    while i < len(words):
        current_word = words[i].lower()
        
        # Try multi-word combinations (up to 4 words)
        for length in range(4, 0, -1):
            if i <= len(words) - length:
                phrase = ' '.join(word.lower() for word in words[i:i+length])
                if phrase in sql_keywords or phrase in comparison_words:
                    tokens.append(phrase)
                    i += length
                    break
        else:
            # Handle single words
            if current_word not in joining_words:
                if (current_word in sql_keywords or 
                    current_word in aggregation_words or 
                    current_word in comparison_words):
                    tokens.append(current_word)
                else:
                    tokens.append(words[i])  # Preserve original case
            i += 1
    
    return tokens

def natural_language_to_sql(input_text):
    """Enhanced SQL converter with support for complex queries."""
    tokens = tokenize_input(input_text)
    
    # Initialize query components
    select_columns = []
    table_name = get_last_uploaded_table()
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
            if sql_keywords[token] in ['JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN']:
                current_clause = 'JOIN'
                current_join_type = sql_keywords[token]
            elif sql_keywords[token] == 'ON':
                current_clause = 'ON'
            elif sql_keywords[token] == 'HAVING':
                current_clause = 'HAVING'
                in_having = True
            else:
                current_clause = sql_keywords[token]
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
                        current_column = col_expr
                    else:
                        select_columns.append(col_expr)
                    i += 2
                    continue
            elif token not in sql_keywords and token != "select":
                select_columns.append(token)
        
        elif current_clause == 'JOIN':
            current_join_table = token
            joins.append({'type': current_join_type, 'table': token, 'condition': None})
        
        elif current_clause == 'ON':
            # Handle join conditions
            if not current_join_condition:
                current_join_condition = token
            elif token in comparison_words:
                comparison = comparison_words[token]
                if i + 1 < len(tokens):
                    joins[-1]['condition'] = f"{current_join_condition} {comparison} {tokens[i+1]}"
                    i += 2
                    current_join_condition = None
                    continue
        
        elif current_clause in ['WHERE', 'HAVING']:
            conditions_list = having_conditions if in_having else where_conditions
            
            if token in aggregation_words:
                # Handle aggregation in HAVING clause
                agg_function = aggregation_words[token]
                if i + 2 < len(tokens):
                    agg_column = tokens[i + 1]
                    i += 2
                    if tokens[i] in comparison_words:
                        comparison = comparison_words[tokens[i]]
                        value = tokens[i + 1]
                        conditions_list.append(
                            f"{agg_function}({agg_column}) {comparison} {value}"
                        )
                        i += 2
                        continue
            else:
                # Normal WHERE/HAVING condition
                column = token
                if i + 2 < len(tokens):
                    if tokens[i + 1] in comparison_words:
                        comparison = comparison_words[tokens[i + 1]]
                        value = tokens[i + 2]
                        if value.lower() not in sql_keywords:
                            try:
                                float(value)
                                conditions_list.append(f"{column} {comparison} {value}")
                            except ValueError:
                                conditions_list.append(f"{column} {comparison} '{value}'")
                            i += 3
                            continue
        
        elif current_clause == 'GROUP BY':
            if token not in sql_keywords:
                group_by.append(token)
        
        elif current_clause == 'ORDER BY':
            if token not in sql_keywords:
                order_component = token
                if i + 1 < len(tokens) and tokens[i + 1].lower() in ['asc', 'desc']:
                    order_component += f" {tokens[i + 1].upper()}"
                    i += 1
                order_by.append(order_component)
        
        i += 1
    
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
    
    return " ".join(query_parts)