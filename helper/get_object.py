import re
from helper.validation import validate_string

def get_columns_from_select(query):
    query = query.strip()

    if not query.upper().startswith("SELECT "):
        raise Exception("Query does not start with SELECT.")

    match = re.match(r'^\s*SELECT\s+(.*?)\s+FROM\s', query, re.IGNORECASE)
    if not match:
        raise Exception("Could not find columns in SELECT clause.")

    columns_part = match.group(1).strip()

    return columns_part


def get_condition_from_where(query):
    query = query.strip()

    if "WHERE" not in query.upper():
        raise Exception("No WHERE clause in the query.")

    match = re.search(r'WHERE\s+(.*?)(?=\s*(GROUP BY|ORDER BY|LIMIT|;$))', query, re.IGNORECASE)
    if not match:
        raise Exception("Could not extract the condition from WHERE clause.")

    condition = match.group(1).strip()

    return condition


def get_column_from_group_by(query):
    query = query.strip()

    if "GROUP BY" not in query.upper():
        raise Exception("No GROUP BY clause in the query.")

    match = re.search(r'GROUP BY\s+(.*?)(?=\s*(ORDER BY|LIMIT|;$))', query, re.IGNORECASE)
    if not match:
        raise Exception("Could not extract the column from GROUP BY clause.")

    column = match.group(1).strip()

    return column


def get_column_from_order_by(query):
    query = query.strip()

    if "ORDER BY" not in query.upper():
        raise Exception("No ORDER BY clause in the query.")

    match = re.search(r'ORDER BY\s+(.*?)(?=\s*(LIMIT|;$))', query, re.IGNORECASE)
    if not match:
        raise Exception("Could not extract the column from ORDER BY clause.")

    column = match.group(1).strip()

    return column

def get_operator_operands_from_condition(condition):
    allowed_operators = ["<=", ">=", "<>", "<", ">", "="]
    for operator in allowed_operators:
        if operator in condition:
            left_operand, right_operand = map(str.strip, condition.split(operator, 1))
            return operator, left_operand, right_operand

    raise ValueError(
        f"Invalid condition '{condition}'. Allowed operators are: {', '.join(allowed_operators)}"
    )

def get_table_column_from_operand(operand, columns, stats):
    if operand.isnumeric() or validate_string(operand):
        return None, None

    if "." in operand:
        table_name, column_name = operand.split(".")
        if table_name not in stats or column_name not in stats[table_name]["v_a_r"]:
            raise ValueError(f"Column '{operand}' not found.")
        return table_name, column_name

    matching_columns = [col for col in columns if col.split(".")[1] == operand]
    if len(matching_columns) == 0:
        raise ValueError(
            f"Invalid operand '{operand}'. Must be a number, a string in quotes, or an attribute."
        )
    elif len(matching_columns) > 1:
        raise ValueError(f"Ambiguous column name '{operand}': matches {matching_columns}")
    else:
        return matching_columns[0].split(".")

def get_limit(query):
    query = query.strip()

    if "LIMIT" not in query.upper():
        raise Exception("No LIMIT clause in the query.")

    match = re.search(r'LIMIT\s+(.*?)(?=\s*(;|$))', query, re.IGNORECASE)
    if not match:
        raise Exception("Could not extract the limit value.")

    limit = int(match.group(1))

    return limit


def get_from_table(query):
    query = query.strip()

    if not query.upper().startswith("SELECT "):
        raise Exception("Query does not start with SELECT.")

    match = re.search(r'FROM\s+(.*?)(?=\s*(WHERE|GROUP BY|ORDER BY|LIMIT|;$))', query, re.IGNORECASE)
    if not match:
        raise Exception("Could not extract the table name from FROM clause.")

    table = match.group(1).strip()

    return table


def extract_joins(query):
    join_pattern = r'([^\s]+)\s+(NATURAL\s+|LEFT\s+|RIGHT\s+|INNER\s+|OUTER\s+|CROSS\s+|)?JOIN\s+([^\s]+)(?:\s+ON\s+([^\s]+))?'
    matches = re.findall(join_pattern, query, re.IGNORECASE)
    result = []

    for left_table, join_type, right_table, on_condition in matches:
        join_type = join_type.strip() if join_type else ''
        join_expression = f"{left_table} {join_type}join {right_table}".strip()

        result.append(join_expression)

    return result


def extract_on_conditions(query):
    on_condition_pattern = r'\bON\s+([^;]+?)\s*(?=\b(JOIN|WHERE|GROUP BY|ORDER BY|LIMIT|$))'

    matches = re.findall(on_condition_pattern, query, re.IGNORECASE)
    on_conditions = [match[0].strip() for match in matches]

    return on_conditions


def extract_set_conditions(query):
    set_pattern = r"SET\s+(.+?)\s+WHERE"
    match = re.search(set_pattern, query, re.IGNORECASE)

    if not match:
        return []

    set_clause = match.group(1).strip()
    conditions = [condition.strip() for condition in set_clause.split(',')]

    return conditions


def extract_table_update(query):
    query = query.strip()

    if not query.upper().startswith("UPDATE "):
        raise Exception("Query does not start with UPDATE.")

    match = re.match(r'^\s*UPDATE\s+(.*?)\s+SET\s', query, re.IGNORECASE)
    if not match:
        raise Exception("Could not find columns in UPDATE clause.")

    columns_part = match.group(1).strip()

    return columns_part