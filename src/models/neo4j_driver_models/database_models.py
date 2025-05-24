from dataclasses import dataclass
from typing import List, Dict

from utils.enums import Label


@dataclass
class Node:
    """
    Represents a node in the Neo4j database.
    """

    labels: List[Label]
    properties: Dict[str, any]

    def __str__(self):
        return f"Node(labels={self.labels}, properties={self.properties})"
