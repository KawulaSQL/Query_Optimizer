from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class QueryTree:
    type: str
    val : str
    condition: str
    child: List['QueryTree'] = field(default_factory=list)
    parent: Optional['QueryTree'] = None
    total_block: int = 0
    total_row: int = 0
    columns: List[str] = field(default_factory=list)


@dataclass
class ParsedQuery:
    query: str
    query_tree: Optional[QueryTree] = None
