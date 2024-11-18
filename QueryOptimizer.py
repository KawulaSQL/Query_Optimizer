import re

from model.models import ParsedQuery, QueryTree


class QueryOptimizer:
    def __init__(self, query):
        self.query = query
        self.query_tree = None

    def parse(self) -> ParsedQuery:

        if self.query is None:
            raise Exception("Query is not set")

        res = ParsedQuery(self.query)

        if self.query.to_lower() in "select":
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

    def optimize(self) -> ParsedQuery:
        return self.query

    def get_cost(self) -> int:
        return 0


