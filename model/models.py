from dataclasses import dataclass, field
from typing import List


@dataclass
class QueryTree:
    type: str
    val : str
    child: List['QueryTree'] = field(default_factory=list)
    parent: 'QueryTree' = None


@dataclass
class ParsedQuery:
    query: str
    query_tree: QueryTree
