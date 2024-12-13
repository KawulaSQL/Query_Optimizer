from model.models import QueryTree
from helper.get_stats import get_stats
from QueryOptimizer import QueryOptimizer

# long_query = "SELECT m.title, r.rating, a.name AS actor_name, d.name AS director_name, aw.category FROM movies m JOIN reviews r ON m.movie_id = r.movie_id JOIN movie_actors ma ON m.movie_id = ma.movie_id JOIN actors a ON ma.actor_id = a.actor_id JOIN movie_directors md ON m.movie_id = md.movie_id JOIN directors d ON md.director_id = d.director_id LEFT JOIN awards aw ON m.movie_id = aw.movie_id WHERE r.rating > 8 AND a.name LIKE 'John%';"

# """
# SELECT title, rating, award_name FROM movies
# JOIN reviews ON movies.movie_id = reviews.movie_id JOIN awards ON movies.movie_id = awards.award_id 
# WHERE genre = 'Horror' AND award_name = 'Scariest Movie';
# """
# # Projection node: Selecting columns to return
# q_p5 = QueryTree(type="project", val="A", condition="title, rating, award_name", child=[])
# q_s2_5 = QueryTree(type="sigma", val="D", condition="award_name = 'Scariest Movie'", child=[], parent=q_p5)
# q_j2_5 = QueryTree(type="join", val="C", condition="movies.movie_id = awards.movie_id", child=[], parent=q_s2_5)
# q_j1_5 = QueryTree(type="join", val="B", condition="movies.movie_id = reviews.movie_id", child=[], parent=q_j2_5)
# q_s1_5 = QueryTree(type="sigma", val="E", condition="genre = 'horror' ", child=[], parent=q_j1_5)

# # Table nodes: Base tables
# q_t5_movies = QueryTree(type="table", val="movies", condition="", child=[], parent=q_s1_5)
# q_t5_reviews = QueryTree(type="table", val="reviews", condition="", child=[], parent=q_j1_5)
# q_t5_awards = QueryTree(type="table", val="awards", condition="", child=[], parent=q_j2_5)

# # Build the tree
# q_p5.child.append(q_s2_5)             # Projection -> Second Join
# q_s2_5.child.append(q_j2_5)      # Selection on award_name -> Awards table
# q_j2_5.child.append(q_j1_5)           # Second Join -> First Join
# q_j2_5.child.append(q_t5_awards)           # Second Join -> Selection on award_name
# q_j1_5.child.append(q_s1_5)           # First Join -> Selection on genre
# q_j1_5.child.append(q_t5_reviews)     # First Join -> Reviews table
# q_s1_5.child.append(q_t5_movies)      # Selection on genre -> Movies table'

# Join node: a.movie_id = m.movie_id
q_j2 = QueryTree(type="join", val="C", condition="a.movie_id = m.movie_id", child=[])

# Sigma node: a.movie_id > 10
q_s1 = QueryTree(type="sigma", val="A", condition="a.movie_id > 10", child=[], parent=q_j2)

# Join node: m.movie_id = r.movie_id
q_j1 = QueryTree(type="join", val="B", condition="m.movie_id = r.movie_id", child=[], parent=q_j2)

# Table nodes: Base tables for awards, movies, and reviews
q_t_awards = QueryTree(type="table", val="awards as a", condition="", child=[], parent=q_s1)  # Awards table as 'a'
q_t_movies = QueryTree(type="table", val="movies as m", condition="", child=[], parent=q_j1)  # Movies table as 'm'
q_t_reviews = QueryTree(type="table", val="reviews as r", condition="", child=[], parent=q_j1)  # Reviews table as 'r'

# Build the tree
# q_j2.child.append(q_j1)         # Join a.movie_id = m.movie_id -> Second Join
# q_j2.child.append(q_s1)         # Join a.movie_id = m.movie_id -> Selection on a.movie_id
# q_s1.child.append(q_t_awards)
# q_j1.child.append(q_t_movies)   # Join m.movie_id = r.movie_id -> Movies table
# q_j1.child.append(q_t_reviews)  # Join m.movie_id = r.movie_id -> Reviews table

test = QueryOptimizer("SELECT id, department.dept_name FROM student JOIN department ON student.dept_name = department.dept_name;", get_stats())

parse_query = test.parse()
test.print_query_tree(parse_query.query_tree)
print(test.get_cost(parse_query.query_tree))

optimized_query = test.optimize(parse_query)
print("\nOptimized Query Tree:")
test.print_query_tree(optimized_query.query_tree)
print(test.get_cost(optimized_query.query_tree))


# print("------- OPTIMIZER -------")
# optimizer = QueryOptimizer(";")
# optimized_tree = optimizer.optimize_coba(q_p5)
# optimizer.print_query_tree(optimized_tree)