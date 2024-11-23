from helper.get_object import get_limit, get_column_from_order_by, get_column_from_group_by, get_condition_from_where, \
    get_columns_from_select, get_from_table
from helper.get_stats import get_stats
from helper.validation import validate_query
from model.models import ParsedQuery, QueryTree

class QueryOptimizer:
    def __init__(self, query):
        self.query = query
        self.parse_result: ParsedQuery = None

    def parse(self) -> ParsedQuery:

        if self.query is None:
            raise Exception("Query is not set")

        try:
            if not validate_query(self.query):
                raise Exception("Invalid query")

            self.parse_result = ParsedQuery(query=self.query)

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
        
    def get_cost(self, query: QueryTree) -> int:
        stats = get_stats()

        if query.type == "table":
            table_stats = stats.get(query.val)
            return table_stats["b_r"]
        
        if query.type == "sigma":
            child_node = query.child[0]
            return self.get_cost(child_node)

        if query.type == "project":
            child_node = query.child[0]
            return self.get_cost(child_node)
        
        if query.type == "join":
            left_node = query.child[0]
            right_node = query.child[1]

            if left_node.type == "table":
                left_stats = stats.get(left_node.val)
                right_stats = stats.get(right_node.val)

                join_cost = left_stats["n_r"] * right_stats["b_r"] + left_stats["b_r"]
                return join_cost
            else:
                join_cost = 0
                return 0

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

