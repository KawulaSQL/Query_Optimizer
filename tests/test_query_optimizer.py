# How to run in root: python -m unittest tests/test_query_optimizer.py

import unittest
from QueryOptimizer import QueryOptimizer
from model.models import QueryTree, ParsedQuery
from helper.get_stats import get_stats

class TestQueryOptimizer(unittest.TestCase):
    def setUp(self):
        self.optimizer = QueryOptimizer("SELECT * FROM movies WHERE genre = 'Horror';", get_stats())
        self.optimizer_complex = QueryOptimizer(
            "SELECT * FROM movies JOIN reviews ON movies.movie_id = reviews.movie_id WHERE movies.movie_id = 1 AND movies.genre = 'test';", 
            get_stats()
        )

    def test_parse_valid_query(self):
        parsed_query = self.optimizer.parse()
        self.assertIsInstance(parsed_query, ParsedQuery)
        self.assertIsNotNone(parsed_query.query_tree)
        self.assertEqual(parsed_query.query, "SELECT * FROM movies WHERE genre = 'Horror';", get_stats())

    def test_optimize_query(self):
        parsed_query = self.optimizer.parse()
        optimized_query = self.optimizer.optimize(parsed_query)
        self.assertIsInstance(optimized_query, ParsedQuery)
        self.assertIsNotNone(optimized_query.query_tree)

    def test_get_cost(self):
        parsed_query = self.optimizer.parse()
        cost = self.optimizer.get_cost(parsed_query.query_tree)
        self.assertIsInstance(cost, int)
        self.assertEqual(cost, 60)
    
    def test_cost_optimization(self):
        parsed_query = self.optimizer_complex.parse()
        original_cost = self.optimizer_complex.get_cost(parsed_query.query_tree)

        optimized_query = self.optimizer_complex.optimize(parsed_query)
        optimized_cost = self.optimizer_complex.get_cost(optimized_query.query_tree)

        self.assertLessEqual(optimized_cost, original_cost)

if __name__ == '__main__':
    unittest.main()
