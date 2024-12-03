from model.models import QueryTree
from QueryOptimizer import QueryOptimizer

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
SELECT title, rating, description FROM movies JOIN review ON movies.movie_id = reviews.movie_id WHERE genre = 'Horror'
"""
q_p4 = QueryTree(type="project", val="A", condition="title, rating, description", child=list())
q_s4 = QueryTree(type="sigma", val="B", condition="genre = 'Horror'", child=list(), parent=q_p4)
q_j4 = QueryTree(type="join", val="C", condition="movies.movie_id = reviews.movie_id", child=list(), parent=q_s4)
q_t4_movies = QueryTree(type="table", val="movies", condition="", child=list(), parent=q_j4)
q_t4_reviews = QueryTree(type="table", val="reviews", condition="", child=list(), parent=q_j4)

q_p4.child.append(q_s4)  # Projection's child is the selection node
q_s4.child.append(q_j4)  # Selection's child is the join node
q_j4.child.append(q_t4_movies)  # Join's first child is the movies table
q_j4.child.append(q_t4_reviews)  # Join's second child is the reviews table

# long_query = "SELECT m.title, r.rating, a.name AS actor_name, d.name AS director_name, aw.category FROM movies m JOIN reviews r ON m.movie_id = r.movie_id JOIN movie_actors ma ON m.movie_id = ma.movie_id JOIN actors a ON ma.actor_id = a.actor_id JOIN movie_directors md ON m.movie_id = md.movie_id JOIN directors d ON md.director_id = d.director_id LEFT JOIN awards aw ON m.movie_id = aw.movie_id WHERE r.rating > 8 AND a.name LIKE 'John%';"

"""
SELECT title, rating, award_name FROM movies
JOIN reviews ON movies.movie_id = reviews.movie_id JOIN awards ON movies.movie_id = awards.award_id 
WHERE genre = 'Horror' AND award_name = 'Scariest Movie';
"""
# Projection node: Selecting columns to return
q_p5 = QueryTree(type="project", val="A", condition="title, rating, award_name", child=[])
q_s2_5 = QueryTree(type="sigma", val="D", condition="award_name = 'Scariest Movie'", child=[], parent=q_p5)
q_j2_5 = QueryTree(type="join", val="C", condition="movies.movie_id = awards.movie_id", child=[], parent=q_s2_5)
q_j1_5 = QueryTree(type="join", val="B", condition="movies.movie_id = reviews.movie_id", child=[], parent=q_j2_5)
q_s1_5 = QueryTree(type="sigma", val="E", condition="genre = 'horror' ", child=[], parent=q_j1_5)

# Table nodes: Base tables
q_t5_movies = QueryTree(type="table", val="movies", condition="", child=[], parent=q_s1_5)
q_t5_reviews = QueryTree(type="table", val="reviews", condition="", child=[], parent=q_j1_5)
q_t5_awards = QueryTree(type="table", val="awards", condition="", child=[], parent=q_j2_5)

# Build the tree
q_p5.child.append(q_s2_5)             # Projection -> Second Join
q_s2_5.child.append(q_j2_5)      # Selection on award_name -> Awards table
q_j2_5.child.append(q_j1_5)           # Second Join -> First Join
q_j2_5.child.append(q_t5_awards)           # Second Join -> Selection on award_name
q_j1_5.child.append(q_s1_5)           # First Join -> Selection on genre
q_j1_5.child.append(q_t5_reviews)     # First Join -> Reviews table
q_s1_5.child.append(q_t5_movies)      # Selection on genre -> Movies table

test = QueryOptimizer("SELECT * FROM movies WHERE 'b' = 'a' or 'a' = age_rating or title >= 'cuki' or genre <> 'horror' order by genre;")

# test = QueryOptimizer("update movies set genre ='horror' where age_rating = '18+';")

parse_query = test.parse()

# test.print_query_tree(parse_query.query_tree)
# print(test.get_cost(parse_query.query_tree))
# print(parse_query.query_tree)
print(test.optimize(parse_query.query_tree))

# print("------- OPTIMIZER -------")
# optimizer = QueryOptimizer(";")
# optimized_tree = optimizer.optimize_coba(q_p5)
# optimizer.print_query_tree(optimized_tree)