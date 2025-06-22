from enum import Enum


class Label(Enum):
    PERSON = "Person"
    MOVIES = "Movies"


class RelationshipType(Enum):
    KNOWS = "KNOWS"
    WATCH = "WATCH"
