import re


def validate_query(query):
    query = query.strip()
    if not query.endswith(";"):
        return False, "Query must end with a semicolon."

    select_pattern = re.compile(
        r'^\s*SELECT\s+.+?\s+FROM\s+\w+' 
        r'(\s+WHERE\s+.+?)?' 
        r'(\s+GROUP BY\s+.+?)?'
        r'(\s+ORDER BY\s+.+?)?'
        r'(\s+LIMIT\s+\d+)?' 
        r'\s*;$',
        re.IGNORECASE
    )

    other_patterns = {
        "DELETE": re.compile(
            r'^\s*DELETE\s+FROM\s+\w+(\s+WHERE\s+.+?)?\s*;$',
            re.IGNORECASE
        ),
        "INSERT": re.compile(
            r'^\s*INSERT\s+INTO\s+\w+\s*\(.+?\)\s+VALUES\s*\(.+?\)\s*;$',
            re.IGNORECASE
        ),
        "UPDATE": re.compile(
            r'^\s*UPDATE\s+\w+\s+SET\s+.+?(\s+WHERE\s+.+?)?\s*;$',
            re.IGNORECASE
        )
    }

    query_type = query.split(maxsplit=1)[0].upper()

    if select_pattern.match(query):
        clause_order = ["WHERE", "GROUP BY", "ORDER BY", "LIMIT"]
        last_seen_index = -1
        for clause in clause_order:
            clause_pos = query.upper().find(clause)
            if clause_pos != -1:
                if clause_pos < last_seen_index:
                    return False, f"Invalid clause order: {clause} appears out of sequence."
                last_seen_index = clause_pos
        return True, "Valid SELECT query."

    if query_type in other_patterns:
        if other_patterns[query_type].match(query):
            return True, f"{query_type} query is valid."
        else:
            return False, f"Invalid {query_type} query syntax."

    return False, "Unsupported query type."


def validate_columns_table(query_columns):
    pattern = re.compile(
        r'^\s*(\*|'                          
        r'(\w+(\s+AS\s+\w+|\s+as\s+\w+)?'   
        r'(\s*,\s*\w+(\s+AS\s+\w+|\s+as\s+\w+)?)*)'
        r')\s*$'
    )

    return pattern.match(query_columns) is not None


def validate_columns(query_columns):
    pattern = re.compile(
        r'^\s*'
        r'(\w+(\s+AS\s+\w+|\s+as\s+\w+)?' 
        r'(\s*,\s*\w+(\s+AS\s+\w+|\s+as\s+\w+)?)*)' 
        r'\s*$'
    )

    return pattern.match(query_columns) is not None






