from helper.get_object import get_limit, get_column_from_order_by, get_column_from_group_by, get_condition_from_where, \
    get_columns_from_select, get_from_table, extract_set_conditions, extract_table_update
from helper.get_stats import get_stats
from helper.validation import validate_query
from model.models import ParsedQuery, QueryTree


class QueryOptimizer:
    def __init__(self, query):
        self.query = query
        self.parse_result: ParsedQuery = None

    """
    Parse query that will return ParsedQuery object
    please use this function with try except block
    because this function will raise exception if the query is not valid
    """
    def parse(self) -> ParsedQuery:

        if self.query is None:
            raise Exception("Query is not set")

        try:
            if not validate_query(self.query):
                raise Exception("Invalid query")

            self.parse_result = ParsedQuery(query=self.query)

            # Parse SELECT condition
            if self.query.upper().startswith("SELECT"):
                q1, q2, q3, q4, q5, q6, proj = None, None, None, None, None, None, None
                val = "A"

                if get_columns_from_select(self.query) != "*":
                    proj = QueryTree(type="project", val=val, condition=get_columns_from_select(self.query), child=list())
                    val = chr(ord(val) + 1)
                    self.parse_result.query_tree = proj

                if self.query.upper().find("LIMIT") != -1:
                    lim = get_limit(self.query)
                    q1 = QueryTree(type="limit", val=val, condition=lim, child=list())
                    val = chr(ord(val) + 1)

                    if proj is not None:
                        proj.child.append(q1)
                        q1.parent = proj
                    else:
                        self.parse_result.query_tree = q1

                if self.query.upper().find("ORDER BY") != -1:
                    order = get_column_from_order_by(self.query)
                    q2 = QueryTree(type="sort", val=val, condition=order, child=list())

                    if q1 is not None:
                        q1.child.append(q2)
                        q2.parent = q1
                    elif proj is not None:
                        proj.child.append(q2)
                        q2.parent = proj
                    else:
                        self.parse_result.query_tree = q2

                    val = chr(ord(val) + 1)

                if self.query.upper().find("GROUP BY") != -1:
                    group_by = get_column_from_group_by(self.query)
                    q3 = QueryTree(type="group", val=val, condition=group_by, child=list())

                    if q2 is not None:
                        q2.child.append(q3)
                        q3.parent = q2
                    elif q1 is not None:
                        q1.child.append(q3)
                        q3.parent = q1
                    elif proj is not None:
                        proj.child.append(q3)
                        q3.parent = proj
                    else:
                        self.parse_result.query_tree = q3

                    val = chr(ord(val) + 1)

                if self.query.upper().find("WHERE") != -1:
                    where = get_condition_from_where(self.query)
                    where_split = where.split(" AND ")
                    q4 = QueryTree(type="sigma", val=val, condition=where_split[0], child=list())
                    temp_parent = q4
                    val = chr(ord(val) + 1)
                    for i in range(1, len(where_split)):
                        q5 = QueryTree(type="sigma", val=val, condition=where_split[i], child=list(), parent=temp_parent)
                        temp_parent.child.append(q5)
                        temp_parent = q5
                        val = chr(ord(val) + 1)

                    if q3 is not None:
                        q3.child.append(q4)
                        q4.parent = q3
                    elif q2 is not None:
                        q2.child.append(q4)
                        q4.parent = q2
                    elif q1 is not None:
                        q1.child.append(q4)
                        q4.parent = q1
                    elif proj is not None:
                        proj.child.append(q4)
                        q4.parent = proj
                    else:
                        self.parse_result.query_tree = q4

                if self.query.upper().find("JOIN") == -1 and self.query.upper().find("NATURAL JOIN") == -1:
                    from_table = get_from_table(self.query)
                    q6 = QueryTree(type="table", val=from_table, condition="", child=list())

                    if q5 is not None:
                        q5.child.append(q6)
                        q6.parent = q5
                    elif q4 is not None:
                        q4.child.append(q6)
                        q6.parent = q4
                    elif q3 is not None:
                        q3.child.append(q6)
                        q6.parent = q3
                    elif q2 is not None:
                        q2.child.append(q6)
                        q6.parent = q2
                    elif q1 is not None:
                        q1.child.append(q6)
                        q6.parent = q1
                    elif proj is not None:
                        proj.child.append(q6)
                        q6.parent = proj
                    else:
                        self.parse_result.query_tree = q6

                if self.query.upper().find("NATURAL JOIN") != -1:
                    join = get_from_table(self.query)
                    join_split = join.split(" NATURAL JOIN ")
                    join_table1 = join_split[0]
                    join_table2 = join_split[1]
                    q7 = QueryTree(type="natural join", val=val, condition="", child=list())

                    q8 = QueryTree(type="table", val=join_table1, condition="", child=list(), parent=q7)
                    q9 = QueryTree(type="table", val=join_table2, condition="", child=list(), parent=q7)

                    q7.child.append(q8)
                    q7.child.append(q9)

                    q8.parent = q7
                    q9.parent = q7

                    if q6 is not None:
                        q6.child.append(q7)
                        q7.parent = q6
                    elif q5 is not None:
                        q5.child.append(q7)
                        q7.parent = q5
                    elif q4 is not None:
                        q4.child.append(q7)
                        q7.parent = q4
                    elif q3 is not None:
                        q3.child.append(q7)
                        q7.parent = q3
                    elif q2 is not None:
                        q2.child.append(q7)
                        q7.parent = q2
                    elif q1 is not None:
                        q1.child.append(q7)
                        q7.parent = q1
                    elif proj is not None:
                        proj.child.append(q7)
                        q7.parent = proj
                    else:
                        self.parse_result.query_tree = q7

                elif self.query.upper().find("NATURAL JOIN") != -1:
                    join = get_from_table(self.query)
                    join_split = join.split(" JOIN ")
                    join_table1 = join_split[0]
                    join_table2 = join_split[1].split(" ON ")[0]
                    join_condition = join_split[1]
                    q7 = QueryTree(type="join", val=val, condition=join_condition, child=list())

                    q8 = QueryTree(type="table", val=join_table1, condition="", child=list(), parent=q7)
                    q9 = QueryTree(type="table", val=join_table2, condition="", child=list(), parent=q7)

                    q7.child.append(q8)
                    q7.child.append(q9)

                    q8.parent = q7
                    q9.parent = q7

                    if q6 is not None:
                        q6.child.append(q7)
                        q7.parent = q6
                    elif q5 is not None:
                        q5.child.append(q7)
                        q7.parent = q5
                    elif q4 is not None:
                        q4.child.append(q7)
                        q7.parent = q4
                    elif q3 is not None:
                        q3.child.append(q7)
                        q7.parent = q3
                    elif q2 is not None:
                        q2.child.append(q7)
                        q7.parent = q2
                    elif q1 is not None:
                        q1.child.append(q7)
                        q7.parent = q1
                    elif proj is not None:
                        proj.child.append(q7)
                        q7.parent = proj
                    else:
                        self.parse_result.query_tree = q7

            # Parse UPDATE condition
            elif self.query.upper().startswith("UPDATE"):
                set_condition = extract_set_conditions(self.query)
                update_table = extract_table_update(self.query)
                where_condition = get_condition_from_where(self.query)

                q1, q2, q3 = None, None, None
                val = "A"
                temp_parent = None

                for condition in set_condition:
                    q1 = QueryTree(type="update", val=val, condition=condition, child=list())
                    if temp_parent is not None:
                        temp_parent.child.append(q1)
                        q1.parent = temp_parent
                    else:
                        self.parse_result.query_tree = q1
                    temp_parent = q1
                    val = chr(ord(val) + 1)

                if where_condition:
                    where_condition_split = where_condition.split(" AND ")
                    q2 = QueryTree(type="sigma", val=val, condition=where_condition_split[0], child=list())
                    temp_parent = q2
                    val = chr(ord(val) + 1)
                    for i in range(1, len(where_condition_split)):
                        q3 = QueryTree(type="sigma", val=val, condition=where_condition_split[i], child=list(), parent=temp_parent)
                        temp_parent.child.append(q3)
                        temp_parent = q3
                        val = chr(ord(val) + 1)
                    q1.child.append(q2)
                    q2.parent = q1

                q4 = QueryTree(type="table", val=update_table, condition="", child=list())
                if q3 is not None:
                    q3.child.append(q4)
                    q4.parent = q3
                elif q2 is not None:
                    q2.child.append(q4)
                    q4.parent = q2
                else:
                    q1.child.append(q4)
                    q4.parent = q1

        except Exception as e:
            raise Exception(f"Error parsing query: {str(e)}")

        return self.parse_result

    def optimize(self, query: ParsedQuery) -> ParsedQuery:
        if query.query_tree is None:
            raise Exception("Query is not set")
        
        process_query = [query.query_tree] # start with root node
        
        while process_query:
            node = process_query.pop() # process the current node
            
            if node.type == "sigma":
                if len(node.child) == 1 and node.child[0].type in ["join", "table"]:
                    child_node = node.child[0]
                    
                    if child_node.type == "join":
                    # Split kondisi seleksi untuk join
                        conditions = node.condition.split(" AND ")
                        left_conditions = []
                        right_conditions = []
                        other_conditions = []
                        
                        if left_conditions:
                            left_sigma = QueryTree(
                                type="sigma",
                                val=node.val + "_L",
                                condition=" AND ".join(left_conditions),
                                child=[child_node.child[0]],
                                parent=child_node,
                            )
                            child_node.child[0].parent = left_sigma
                            child_node.child[0] = left_sigma 
                        
                        if right_conditions:
                            right_sigma = QueryTree(
                                type="sigma",
                                val=node.val + "_R",
                                condition=" AND ".join(right_conditions),
                                child=[child_node.child[1]],
                                parent=child_node,
                            )
                            child_node.child[1].parent = right_sigma
                            child_node.child[1] = right_sigma
                            
                        if other_conditions:
                            node.condition = " AND ".join(other_conditions)
                        else:
                            node.child = child_node.child
                            node.type = child_node.type
                            node.condition = child_node.condition
                            node.val = child_node.val
            
    def get_cost(self, qt: QueryTree) -> int:
        stats = get_stats()

        if qt.type == "table":
            table_stats = stats.get(qt.val)
            if not table_stats:
                raise ValueError(f"Table '{qt.val}' not found in stats.")
            
            qt.total_row = table_stats["n_r"]
            qt.total_block = table_stats["b_r"]
            qt.columns = [f"{qt.val}.{col}" for col in stats[qt.val]["v_a_r"].keys()]

            return qt.total_block

        # todo handle range based condition
        if qt.type == "sigma":
            child_node = qt.child[0]
            child_cost = self.get_cost(child_node)

            qt.columns = child_node.columns

            conditions = [cond.strip() for cond in qt.condition.lower().split(" or ")]

            qt.total_row = 0

            for condition in conditions:
                condition_column = condition.split("=")[0].strip()

                if "." not in condition_column:
                    matching_columns = [col for col in child_node.columns if col.split(".")[1] == condition_column]
                    
                    if len(matching_columns) == 0:
                        raise ValueError(f"Column '{condition_column}' not found in the child's columns: {child_node.columns}")
                    elif len(matching_columns) > 1:
                        raise ValueError(f"Ambiguous column name '{condition_column}': matches {matching_columns}")
                    else:
                        condition_column = matching_columns[0]

                table_name, column_name = condition_column.split(".")
                
                if table_name not in stats or column_name not in stats[table_name]["v_a_r"]:
                    raise ValueError(f"Column '{condition_column}' not found in stats.")
                
                distinct_values = stats[table_name]["v_a_r"][column_name]

                qt.total_row += round((child_node.total_row - qt.total_row) / distinct_values)

            return child_cost

        if qt.type == "project":
            child_node = qt.child[0]
            child_cost = self.get_cost(child_node)

            projection_columns = [col.strip() for col in qt.condition.split(",")]

            validated_columns = []

            for column in projection_columns:
                if "." in column:
                    if column not in child_node.columns:
                        raise ValueError(f"Column '{column}' is not present in the child's columns: {child_node.columns}")
                    validated_columns.append(column)
                else:
                    matching_columns = [col for col in child_node.columns if col.split(".")[1] == column]
                    
                    if len(matching_columns) == 0:
                        raise ValueError(f"Column '{column}' is not found in the child's columns: {child_node.columns}")
                    elif len(matching_columns) > 1:
                        raise ValueError(f"Ambiguous column name '{column}': matches {matching_columns}")
                    else:
                        validated_columns.append(matching_columns[0])

            qt.columns = validated_columns

            return child_cost

        # todo validate join condition
        if qt.type == "join":
            left_node = qt.child[0]
            right_node = qt.child[1]

            left_node_cost = self.get_cost(left_node)

            if right_node.type == "table":
                table_stats = stats.get(right_node.val)
                if not table_stats:
                    raise ValueError(f"Table '{right_node.val}' not found in stats.")
                
                right_node.total_row = table_stats["n_r"]
                right_node.total_block = table_stats["b_r"]
                right_node.columns = [f"{right_node.val}.{col}" for col in stats[right_node.val]["v_a_r"].keys()]

            # todo determine total row by fk
            qt.columns = list(set(left_node.columns) | set(right_node.columns))
            qt.total_row = max(right_node.total_row, left_node.total_row)
            join_cost = left_node_cost + left_node.total_row * right_node.total_block
            
            return join_cost

    def print_query_tree(self, node, depth=0):
        if node is None:
            return
        
        indent = "--" * depth + "> "
        if node.type == "project":
            print(f"{indent}project {node.condition}")
        elif node.type == "sigma":
            print(f"{indent}sigma {node.condition}")
        elif node.type == "join":
            print(f"{indent}join {node.condition}")
        elif node.type == "table":
            print(f"{indent}table {node.val}")
        
        for child in node.child:
            self.print_query_tree(child, depth + 1)

