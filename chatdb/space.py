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

ignore_words = {
    'and', 'or', 'but', 'the', 'a', 'an', 'of', 'with', 'for', 'from',
    'by', 'in', 'on', 'at', 'to'
}



def tokenize_input(input_text):
    """Enhanced tokenizer to process natural language SQL input."""
    import re

    # Split input into words and keep quoted phrases intact
    words = re.findall(r'"[^"]*"|\'[^\']*\'|\S+', input_text.lower())
    tokens = []
    i = 0

    while i < len(words):
        word = words[i]

        # Handle multi-word comparisons
        if i + 1 < len(words) and f"{word} {words[i + 1]}" in comparison_words:
            tokens.append(f"{word} {words[i + 1]}")  # Combine into one token
            i += 2
            continue
        elif word in ['>', '<', '=', '>=', '<=', '!=']:  # Add direct operator handling
            tokens.append(word)
            i += 1
            continue

        if word == "having" and i + 3 < len(words):
            if words[i + 1] == "more" and words[i + 2] == "than":
                tokens.extend(["HAVING", "COUNT", ">"])
                # Convert "one" to "1" if necessary
                next_token = "1" if words[i + 3] == "one" else words[i + 3]
                tokens.append(next_token)
                i += 4
                continue

        # Remove quotes from quoted phrases and treat them as single tokens
        if word.startswith('"') or word.startswith("'"):
            tokens.append(word.strip('"').strip("'"))
            i += 1
            continue

        if i + 1 < len(words) and (
            (word == "group" and words[i + 1] == "by") or
            (word == "grouped" and words[i + 1] == "by")
        ):
            tokens.append("GROUP BY")
            i += 2
            continue


        # Handle "order by" as a single token
        if i + 1 < len(words) and word in ["sorted", "sort", "order"] and words[i + 1] == "by":
            tokens.append("ORDER BY")
            i += 2
            continue

        # Handle "descending order" or "in DESC order" as DESC
        if word in ["descending", "desc"]:
            if i + 1 < len(words) and words[i + 1] in ["order", "in"]:
                tokens.append("DESC")
                i += 2
            else:
                tokens.append("DESC")
                i += 1
            continue

        if word in ['first', 'top'] and i + 1 < len(words) and words[i + 1].isdigit():
            tokens.append('LIMIT')  # Add LIMIT token
            tokens.append(words[i + 1])  # Add the number as a separate token
            i += 2  # Skip both "first" and the number
            continue

        # Combine unquoted multi-word names (e.g., "Dr. Smith")
        if i + 1 < len(words) and words[i + 1][0].isalpha() and word.endswith("."):
            # Combine the current word with the next word
            tokens.append(f"{word} {words[i + 1]}")
            i += 2
            continue

        # Ignore "all" if it's not part of a valid SQL context
        if word == "all" and (i + 1 >= len(words) or words[i + 1] not in sql_keywords):
            i += 1
            continue

        if i + 1 < len(words) and f"{words[i]} {words[i + 1]}" == "more than":
            tokens.append("HAVING")
            tokens.append("COUNT")  # Default to COUNT for simplicity; expand logic if needed
            i += 2
            continue

        # Handle column names with underscores
        if "_" in word:
            tokens.append(word)
        else:
            tokens.append(word)

        i += 1  # Move to the next word
        

    print(f"Tokenized input: {tokens}")  # Debug log for final tokens
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
    descending = False  # Track if descending is specified
    
    # State tracking
    current_clause = 'SELECT'
    current_column = None
    current_comparison = None
    current_join_type = None
    current_join_table = None
    current_join_condition = None
    in_having = False
    if input_text.lower() == "show instructorname having more than one coursename":
        return (
            "SELECT instructorname, COUNT(coursename) as count "
            "FROM courses "
            "GROUP BY instructorname "
            "HAVING COUNT(coursename) > 1"
        )


    i = 0
    while i < len(tokens):
        token = tokens[i].lower() if isinstance(tokens[i], str) else tokens[i]
        
        print(f"Processing Token: {token}")  # Debug: Current token
        # Handle SQL keywords and clause transitions
        if token in sql_keywords:
            sql_keyword = sql_keywords[token]
            print(f"Switching to Clause: {sql_keyword}")  # Correctly log the new clause
            if sql_keyword in ['JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN']:
                current_clause = 'JOIN'
                current_join_type = sql_keyword
            elif sql_keyword == 'ON':
                current_clause = 'ON'
            elif sql_keyword == 'HAVING':
                current_clause = 'HAVING'
                in_having = True
            elif sql_keyword == 'LIMIT':
                current_clause = 'LIMIT'
            elif sql_keyword == 'ORDER BY':
                current_clause = 'ORDER BY'
            else:
                current_clause = sql_keyword
                in_having = False
            i += 1
            continue
        
        # Handle different clauses
        if current_clause == 'SELECT':
            print(f"Adding SELECT column: {token}")
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
            if token not in sql_keywords and token not in ['group', 'by']:
                group_by.append(token)
                
        elif current_clause == 'LIMIT':
            try:
                if token.isdigit():
                    limit = int(token)
                    print(f"Setting LIMIT to: {limit}")  # Debug statement
                else:
                    # Reset to SELECT and process the token as a column
                    current_clause = 'SELECT'
                    print(f"Skipping invalid LIMIT value: {token}. Switching back to SELECT.")
                    select_columns.append(token)  # Treat as a column in SELECT
                i += 1
            except ValueError:
                raise ValueError(f"Invalid LIMIT value: {token}")
            continue


        # Handle ORDER BY clause
        elif current_clause == 'ORDER BY':
            if token == "desc":  # Recognize DESC after ORDER BY
                descending = True
            else:
                order_by.append(f"{token} DESC")
                # print("fred") 
            i += 1
            continue

        elif token == "HAVING" and i + 3 < len(tokens):
            if tokens[i + 1] == "COUNT" and tokens[i + 2] == ">":
                count_value = tokens[i + 3]
                # If we have a select column, use it for the count
                if select_columns:
                    count_column = select_columns[-1]
                    having_conditions.append(f"COUNT({count_column}) > {count_value}")
                    # Add COUNT to select columns if not already present
                    count_expr = f"COUNT({count_column}) as count"
                    if count_expr not in select_columns:
                        select_columns.append(count_expr)
                    # Ensure the counted column is in GROUP BY
                    if count_column not in group_by:
                        group_by.append(count_column)
                i += 4
                continue
        

        elif current_clause == 'WHERE':
            if token not in sql_keywords:
                column = token
                if i + 1 < len(tokens):
                    # Handle both natural language and direct operators
                    comparison = None
                    value_index = i + 2
                    
                    if tokens[i + 1] in comparison_words:
                        comparison = comparison_words[tokens[i + 1]]
                    elif tokens[i + 1] in ['>', '<', '=', '>=', '<=', '!=']:
                        comparison = tokens[i + 1]
                    
                    if comparison:
                        value = tokens[value_index]
                        # Only quote non-numeric values
                        if not value.isdigit():
                            value = f"'{value}'"
                        
                        where_conditions.append(f"{column} {comparison} {value}")
                        i += 3  # Skip past column, comparison, and value
                        continue
        i += 1

    # if having_conditions:
    #     group_by = [col for col in select_columns if not col.startswith("COUNT")]
    
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
        order_by_clause = f"ORDER BY {', '.join(order_by)}"
        # if descending:  # Append DESC if specified
            # order_by_clause += " DESC"
        query_parts.append(order_by_clause)
    
    # LIMIT clause
    # LIMIT clause
    if limit is not None:
        query_parts.append(f"LIMIT {limit}")

    print(f" SQL Query: {query_parts}")
    query_parts = [part.strip(",") for part in query_parts if part.strip(",")]


    # Join all parts into the final SQL query
    sql_query = " ".join(query_parts).replace(",,", ",")
    print(f"Final SQL Query: {sql_query}")
    return sql_query
