# How to run in root: python -m unittest tests/test_query_optimizer.py

import unittest
from QueryOptimizer import QueryOptimizer
from model.models import QueryTree, ParsedQuery

class TestQueryOptimizer(unittest.TestCase):
    def setUp(self):
        self.optimizer = QueryOptimizer("SELECT * FROM movies WHERE genre = 'Horror';")
        self.optimizer_invalid = QueryOptimizer("SELECT * FROM movies WHERE 'b' = 'a' or 'a' = age_rating;")

    def test_parse_valid_query(self):
        parsed_query = self.optimizer.parse()
        self.assertIsInstance(parsed_query, ParsedQuery)
        self.assertIsNotNone(parsed_query.query_tree)
        self.assertEqual(parsed_query.query, "SELECT * FROM movies WHERE genre = 'Horror';")

    def test_parse_invalid_query(self):
        parsed_query = self.optimizer_invalid.parse()
        self.assertIsInstance(parsed_query, ParsedQuery)
        self.assertIsNotNone(parsed_query.query_tree)
        self.assertEqual(parsed_query.query, "SELECT * FROM movies WHERE 'b' = 'a' or 'a' = age_rating;")

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

    def test_print_query_tree(self):
        parsed_query = self.optimizer.parse()
        with self.assertLogs('QueryOptimizer', level='INFO') as log:
            self.optimizer.print_query_tree(parsed_query.query_tree)
        self.assertTrue(any("Node:" in message for message in log.output))

    def test_query_tree_structure(self):
        q_p = QueryTree(type="project", val="A", condition="title, rating", child=[])
        q_s = QueryTree(type="sigma", val="B", condition="genre = 'Horror'", child=[], parent=q_p)
        q_t = QueryTree(type="table", val="movies", condition="", child=[], parent=q_s)

        q_p.child.append(q_s)
        q_s.child.append(q_t)

        self.assertEqual(q_p.type, "project")
        self.assertEqual(q_s.type, "sigma")
        self.assertEqual(q_t.type, "table")
        self.assertEqual(q_p.child[0], q_s)
        self.assertEqual(q_s.child[0], q_t)

if __name__ == '__main__':
    unittest.main()
