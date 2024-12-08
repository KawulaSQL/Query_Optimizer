from helper.get_object import get_limit, get_column_from_order_by, get_column_from_group_by, get_condition_from_where, \
    get_columns_from_select, get_from_table, extract_set_conditions, extract_table_update, get_operator_operands_from_condition, get_table_column_from_operand
from helper.get_stats import get_stats
from helper.validation import validate_query, validate_string
from helper.optimizes import generate_query_plans
from model.models import ParsedQuery, QueryTree
import math


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

                elif self.query.upper().find("JOIN") != -1:
                    join = get_from_table(self.query)
                    join_split = join.split(" JOIN ")
                    join_table1 = join_split[0]
                    join_table2 = join_split[1].split(" ON ")[0]
                    join_condition = join_split[1].split(" ON ")[1]
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
    
    def optimize(self, parsed_query: ParsedQuery) -> ParsedQuery:
        if parsed_query is None or parsed_query.query_tree is None:
            return parsed_query

        query_plans = generate_query_plans(self, parsed_query.query_tree)
        
        min_cost = float('inf')
        best_plan = parsed_query.query_tree
        
        for plan in query_plans:
            cost = self.get_cost(plan)
            if cost < min_cost:
                min_cost = cost
                best_plan = plan
                
        return ParsedQuery(query=parsed_query.query, query_tree=best_plan)

    # def optimize(self, query_tree: QueryTree) -> QueryTree:
    #     nodes_to_process = [query_tree]

    #     while nodes_to_process:
    #         current_node = nodes_to_process.pop()

    #         if current_node.type == "sigma":
    #             if len(current_node.child) == 1 and current_node.child[0].type in ["join", "table"]:
    #                 child_node = current_node.child[0]

    #                 if child_node.type == "join":
    #                     conditions = current_node.condition.split(" AND ")
    #                     left_conditions, right_conditions, other_conditions = [], [], []

    #                     for condition in conditions:
    #                         column = condition.split("=")[0].strip()

                      
    #                         if column.startswith(child_node.child[0].val + "."):
    #                             left_conditions.append(condition)
    #                         elif column.startswith(child_node.child[1].val + "."):
    #                             right_conditions.append(condition)
    #                         else:
    #                             other_conditions.append(condition)

    #                     if left_conditions:
    #                         left_sigma = QueryTree(
    #                             type="sigma",
    #                             val=f"{current_node.val}_L",
    #                             condition=" AND ".join(left_conditions),
    #                             child=[child_node.child[0]],
    #                             parent=child_node,
    #                         )
    #                         child_node.child[0].parent = left_sigma
    #                         child_node.child[0] = left_sigma

    #                     if right_conditions:
    #                         right_sigma = QueryTree(
    #                             type="sigma",
    #                             val=f"{current_node.val}_R",
    #                             condition=" AND ".join(right_conditions),
    #                             child=[child_node.child[1]],
    #                             parent=child_node,
    #                         )
    #                         child_node.child[1].parent = right_sigma
    #                         child_node.child[1] = right_sigma

    #                     if other_conditions:
    #                         current_node.condition = " AND ".join(other_conditions)
    #                     else:
    #                         current_node.type = child_node.type
    #                         current_node.condition = child_node.condition
    #                         current_node.val = child_node.val
    #                         current_node.child = child_node.child
    #         nodes_to_process.extend(current_node.child)
            
    #     return query_tree

    """
    Get cost of a query plan by checking the cost of each node in the query tree. Also performs syntax validation on the query structure and 
    makes sure that attributes that are referenced are valid. The function uses the get_stats function from the storage manager to help estimate
    the cost of an operation.
    """
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

        if qt.type == "sigma":
            child_node = qt.child[0]
            child_cost = self.get_cost(child_node)

            qt.columns = child_node.columns

            conditions = [cond.strip() for cond in qt.condition.lower().split(" or ")]

            qt.total_row = 0

            for condition in conditions:
                column_name = None
                operator, left_operand, right_operand = get_operator_operands_from_condition(condition)

                table_name, column_name = get_table_column_from_operand(left_operand, child_node.columns, stats)
                if column_name is None:
                    table_name, column_name = get_table_column_from_operand(right_operand, child_node.columns, stats)
                
                if column_name is not None:
                    if operator == "=":
                        distinct_values = stats[table_name]["v_a_r"].get(column_name, 1)
                        qt.total_row += round((child_node.total_row - qt.total_row) / distinct_values)
                    elif operator == "<>":
                        distinct_values = stats[table_name]["v_a_r"].get(column_name, 1)
                        qt.total_row += (child_node.total_row - qt.total_row) - round((child_node.total_row - qt.total_row) / distinct_values)
                    else:
                        qt.total_row += (child_node.total_row - qt.total_row) / 2
                else:
                    if (left_operand.isnumeric() and validate_string(right_operand)) or (validate_string(left_operand) and right_operand.isnumeric()):
                        raise ValueError(f"Incompatible operand types: {left_operand} and {right_operand}.")
                    if left_operand == right_operand:
                        qt.total_row = child_node.total_row

                if qt.total_row == child_node.total_row:
                    break

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

        if qt.type == "join" or qt.type == "natural join":
            left_node = qt.child[0]
            right_node = qt.child[1]

            left_node_cost = self.get_cost(left_node)

            table_stats = stats.get(right_node.val)
            if not table_stats:
                raise ValueError(f"Table '{right_node.val}' not found in stats.")
            
            right_node.total_row = table_stats["n_r"]
            right_node.total_block = table_stats["b_r"]
            right_node.columns = [f"{right_node.val}.{col}" for col in stats[right_node.val]["v_a_r"].keys()]

            combined_columns = set(left_node.columns) | set(right_node.columns)
            conditions = [cond.strip() for cond in qt.condition.lower().split(" or ")]

            for condition in conditions:
                column_name = None
                operator, left_operand, right_operand = get_operator_operands_from_condition(condition)

                table_name, column_name = get_table_column_from_operand(left_operand, combined_columns, stats)
                if column_name is None:
                    table_name, column_name = get_table_column_from_operand(right_operand, combined_columns, stats)
                
                if column_name is None and (left_operand.isnumeric() and validate_string(right_operand)) or (validate_string(left_operand) and right_operand.isnumeric()):
                        raise ValueError(f"Incompatible operand types: {left_operand} and {right_operand}.")
                    
            qt.columns = list(set(left_node.columns) | set(right_node.columns))
            qt.total_row = max(right_node.total_row, left_node.total_row)
            join_cost = left_node_cost + left_node.total_row * right_node.total_block
            
            return join_cost
        
        if qt.type == "limit":
            child_node = qt.child[0]
            child_cost = self.get_cost(child_node)
            
            return child_cost + 1
        
        if qt.type == "sort":
            child_node = qt.child[0]
            child_cost = self.get_cost(child_node)

            if child_node.type == "table":
                block_size = stats[child_node.val]["f_r"]
                total_rows = child_node.total_row

                num_blocks = (total_rows + block_size - 1) // block_size

                initial_pass_cost = num_blocks * 2

                b_b = 3
                merge_passes = math.ceil(math.log(num_blocks / b_b, 2)) if num_blocks > b_b else 0

                merge_cost = merge_passes * num_blocks

                sort_cost = initial_pass_cost + merge_cost

                return child_cost + sort_cost
            else:
                return child_cost + 0
        
        if qt.type == "update":
            child_node = qt.child[0]
            child_cost = self.get_cost(child_node)
            
            column = [col.strip() for col in qt.condition.split("=")][0]
            validated_columns = []
            
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
            
            update_cost = child_node.total_row
            return child_cost + update_cost

    def print_query_tree(self, node, depth=0):
        if node is None:
            return

        indent = "--" * depth + "> "
        print(f"{indent}{node.type}, {node.condition}, {node.val}".strip())
        
        for child in node.child:
            self.print_query_tree(child, depth + 1)