

class QueryOptimizer:
    def __init__(self, query):
        self.query = query
        self.query_tree =  None

    def parse(self):
        return self.query

    def optimize(self):
        return self.query

    def get_cost(self):
        return 0