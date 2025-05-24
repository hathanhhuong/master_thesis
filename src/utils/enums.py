from enum import Enum


class Label(Enum):
    MOVIE = "Movie"
    PERSON = "Person"


class RelationshipType(Enum):
    ACTED_IN = "ACTED_IN"
