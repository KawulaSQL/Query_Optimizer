import re

from model.models import ParsedQuery, QueryTree
from typing import Dict, Any

class QueryOptimizer:
    def __init__(self, query):
        self.query = query
        self.query_tree = None

    def parse(self) -> ParsedQuery:

        if self.query is None:
            raise Exception("Query is not set")

        res = ParsedQuery(self.query)

        if self.query.lower() in "select":
            match = re.search(r"SELECT (.+?) FROM", self.query, re.IGNORECASE)
            if match:
                temp = match.group(1)
                column = temp
            else:
                raise Exception("Syntax not valid")

            if column == "":
                raise Exception("Column is not set")

            q1 = QueryTree(type="project", val="A", condition=column, child=list())

            match = re.search(r"WHERE (.+?) ORDER BY", self.query, re.IGNORECASE)

            if match:
                temp = match.group(1)
                condition = temp
            else:
                raise Exception("Syntax not valid")

            if condition == "":
                raise Exception("Condition is not set")

            cons = condition.split("AND")
            temp_par = q1
            val = "B"
            for i, con in cons:
                q2 = QueryTree(type="sigma", val=val, condition=con, child=list(), parent=temp_par)
                temp_par.child.append(q2)
                temp_par = q2
                val = chr(ord(val) + 1)

        return res

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

test = QueryOptimizer("")

print(f"Cost: {test.get_cost(q_p4)}")