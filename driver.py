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
SELECT title, rating, description FROM movies m JOIN reviews r ON m.movie_id = r.review_id WHERE genre = 'Horror'
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

test = QueryOptimizer("SELECT movie_id, title, genre FROM movies WHERE title = 'Insidious' AND genre = 'Horror';")

parse_query = test.parse()

test.print_query_tree(q_p4)
print(test.get_cost(q_p4))
# print(f"Cost: {test.get_cost(q_p4)}")

# print(test.parse())
# print(test.print_query_tree(test.parse().query_tree))