from helper.get_object import get_limit, get_column_from_order_by, get_column_from_group_by, get_condition_from_where, \
    get_columns_from_select, get_from_table
from helper.validation import validate_query
from model.models import ParsedQuery, QueryTree
from typing import Dict, Any


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
                q1, q2, q3, q4, q5, proj = None, None, None, None, None, None
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
                    q5 = QueryTree(type="table", val=from_table, condition="", child=list())

                    if q4 is not None:
                        q4.child.append(q5)
                        q5.parent = q4
                    elif q3 is not None:
                        q3.child.append(q5)
                        q5.parent = q3
                    elif q2 is not None:
                        q2.child.append(q5)
                        q5.parent = q2
                    elif q1 is not None:
                        q1.child.append(q5)
                        q5.parent = q1
                    elif proj is not None:
                        proj.child.append(q5)
                        q5.parent = proj
                    else:
                        self.parse_result.query_tree = q5



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
        stats = self.get_stats()

        if query.type == "table":
            table_stats = stats.get(query.val)
            return table_stats["b_r"]
        
        if query.type == "sigma":
            # Untuk sekarang asumsi linear search, gak ada index
            child_node = query.child[0]
            return self.get_cost(child_node)

        if query.type == "project":
            child_node = query.child[0]
            return self.get_cost(child_node)
        
        if query.type == "join":
            # Untuk sekarang asumsi nested-loop join
            left_node = query.child[0]
            right_node = query.child[1]

            # Kalo table, + b(r). Kalo bukan, gausah.
            if left_node.type == "table":
                left_stats = stats.get(left_node.val)
                right_stats = stats.get(right_node.val)

                join_cost = left_stats["n_r"] * right_stats["b_r"] + left_stats["b_r"]
                return join_cost
            else:
                join_cost = 0 # Belom
                return 0
    
    def get_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Returns statistical data for two tables with specified attributes and a one-to-many relationship.
        """
        stats = {
            "movies": {
                "n_r": 1000,               # Total number of movies (tuples)
                "b_r": 60,                 # Total number of storage blocks
                "l_r": 512,                # Size of a single movie record (in bytes)
                "f_r": 16,                 # Blocking factor (movies per block)
                "v_a_r": {                 # Distinct values for attributes
                    "movie_id": 1000,      # Each movie has a unique ID
                    "title": 980,          # Number of unique movie titles (some may repeat)
                    "genre": 15            # Number of distinct genres
                }
            },
            "reviews": {
                "n_r": 5000,               # Total number of reviews (tuples)
                "b_r": 100,                # Total number of storage blocks
                "l_r": 256,                # Size of a single review record (in bytes)
                "f_r": 50,                 # Blocking factor (reviews per block)
                "v_a_r": {                 # Distinct values for attributes
                    "review_id": 5000,     # Each review has a unique ID
                    "movie_id": 1000,      # Matches the number of movies in the movies table
                    "rating": 10,          # Ratings are distinct values (e.g., 1-10)
                    "description": 4500    # Number of unique review descriptions
                }
            }
        }
        return stats

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

# MOCK DATA

# SELECT nama, alamat FROM mahasiswa WHERE nama = 'budi';
q_p1 = QueryTree(type="project", val="A", condition="nama, alamat", child=list())
q_s1 = QueryTree(type="sigma", val="B", condition="nama = 'budi'", child=list(), parent=q_p1)
# q_p1.child.append(q_s1)

# SELECT nama, alamat, contact
# FROM mahasiswa JOIN kontak ON mahasiswa.id = kontak.id
# WHERE nama = 'budi';
q_p2 = QueryTree(type="project", val="A", condition="nama, alamat, contact", child=list())
q_s2 = QueryTree(type="sigma", val="B", condition="nama = 'budi'", child=list(), parent=q_p2)
q_j2 = QueryTree(type="join", val="C", condition="mahasiswa.id = kontak.id", child=list(), parent=q_s2)
q_p2.child.append(q_s2)
q_s2.child.append(q_j2)
q_t1 = QueryTree(type="table", val="mahasiswa", condition="", child=list(), parent=q_j2)
q_t2 = QueryTree(type="table", val="kontak", condition="", child=list(), parent=q_j2)
q_j2.child.append(q_t1)
q_j2.child.append(q_t2)

# SELECT * FROM movies WHERE genre = 'Horror';
q_s3 = QueryTree(type="sigma", val="A", condition="genre = 'Horror'", child=list())
q_t3 = QueryTree(type="table", val="movies", condition="", child=list(), parent=q_s3)
q_s3.child.append(q_t3)

"""
SELECT title, rating, description 
FROM movies m JOIN reviews r ON m.movie_id = r.review_id
WHERE genre = 'Horror'
"""
# Projection node (select specific columns to return)
q_p4 = QueryTree(type="project", val="A", condition="title, rating, description", child=list())

# Selection node (filter rows where genre = 'Horror')
q_s4 = QueryTree(type="sigma", val="B", condition="genre = 'Horror'", child=list(), parent=q_p4)

# Join node (join movies and reviews on movie_id)
q_j4 = QueryTree(type="join", val="C", condition="movies.movie_id = reviews.movie_id", child=list(), parent=q_s4)

# Adding the join's child tables: movies and reviews
q_t4_movies = QueryTree(type="table", val="movies", condition="", child=list(), parent=q_j4)
q_t4_reviews = QueryTree(type="table", val="reviews", condition="", child=list(), parent=q_j4)

# Connect nodes together to form the tree
q_p4.child.append(q_s4)  # Projection's child is the selection node
q_s4.child.append(q_j4)  # Selection's child is the join node
q_j4.child.append(q_t4_movies)  # Join's first child is the movies table
q_j4.child.append(q_t4_reviews)  # Join's second child is the reviews table

test = QueryOptimizer("SELECT nama, alamat FROM mahasiswa WHERE nama = 'budi' AND kontak = 'anu' LIMIT 10;")

test.print_query_tree(q_p4)
print(f"Cost: {test.get_cost(q_p4)}")

print(test.parse())