import re


def validate_query(query):
    pattern = re.compile(
        r'^\s*'
        r'SELECT\s+'
        r'(\*|' 
        r'(\w+(\s+AS\s+\w+|\s+as\s+\w+)?' 
        r'(\s*,\s*\w+(\s+AS\s+\w+|\s+as\s+\w+)?)*)' 
        r')\s+'
        r'FROM\s+\w+(\s+AS\s+\w+|\s+as\s+\w+)?' 
        r'(\s+JOIN\s+\w+(\s+AS\s+\w+|\s+as\s+\w+)?' 
        r'ON\s+\w+\.\w+\s*=\s*\w+\.\w+)*' 
        r'(\s+WHERE\s+\w+\.\w+\s*(=|<|>|<=|>=|<>)\s*\'\w+\'\s*)*' 
        r'(\s+ORDER\s+BY\s+\w+(\s+ASC|\s+DESC)*)*' 
        r'\s*$'
    )

    return pattern.match(query) is not None


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




