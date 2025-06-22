from dataclasses import dataclass
from typing import List, Dict

from utils.enums import Label


@dataclass
class Node:
    """
    Represents a node in the Neo4j database.
    """

    id: int
    labels: List[Label]
    properties: Dict[str, any]

    def __str__(self):
        return (
            f"Node(id = {self.id}, labels={self.labels}, properties={self.properties})"
        )


@dataclass
class Relationship:
    """
    Represents a relationship in the Neo4j database.
    """

    start_node: Node
    end_node: Node
    relationship_type: str
    properties: Dict[str, any]

    def __str__(self):
        return (
            f"Relationship(start_node={self.start_node}, "
            f"end_node={self.end_node}, "
            f"relationship_type={self.relationship_type}, "
            f"properties={self.properties})"
        )
