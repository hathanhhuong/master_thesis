from dataclasses import dataclass


@dataclass
class ConnectionModel:
    host: str
    user: str
    password: str
