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

    id: int
    start_id: int
    end_id: int
    type: str
    properties: Dict[str, any]

    def __str__(self):
        return (
            f"Relationship(id={self.id}, "
            f"start_id={self.start_id}, "
            f"end_id={self.end_id}, "
            f"type={self.type}, "
            f"properties={self.properties})"
        )
