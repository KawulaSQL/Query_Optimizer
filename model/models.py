from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class QueryTree:
    type: str
    val : str
    condition: str
    child: List['QueryTree'] = field(default_factory=list)
    parent: Optional['QueryTree'] = None


@dataclass
class ParsedQuery:
    query: str
    query_tree: QueryTree
