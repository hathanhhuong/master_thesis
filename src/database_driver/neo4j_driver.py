from typing import Any, Dict, List
from neo4j import GraphDatabase

from logger.logger import Logger
from models.neo4j_driver_models.connection_model import ConnectionModel
from models.neo4j_driver_models.database_models import Node
from utils.constants import NEO4J_DEFAULT_NUMBER_OF_NODES


class Neo4jDriver:
    def __init__(self, logger: Logger):
        self._logger = logger
        self._connection_model: ConnectionModel = None
        self._driver = None

    def _test_connection(self) -> bool:
        """Test the connection to the Neo4j database."""
        try:
            _ = self._driver.execute_query("RETURN 1")
            return True
        except Exception as e:
            return False

    def connect(self, connection_model: ConnectionModel) -> None:
        """Establish a connection to the Neo4j database."""
        try:
            self._connection_model = connection_model
            self._driver = GraphDatabase.driver(
                connection_model.host,
                auth=(connection_model.user, connection_model.password),
            )

            if not self._test_connection():
                raise ValueError("Connection test failed.")

            self._logger.log_info("Successfully connected to Neo4j database.")

        except Exception as e:
            self._logger.log_error(f"Failed to connect to Neo4j: {e}")

    def close(self):
        self._driver.close()

    def execute_query(self, query: str, parameters=None):
        if not self._driver:
            self._logger.log_error("Driver is not initialized. Please connect first.")
            raise RuntimeError("Driver is not initialized. Please connect first.")

        self._logger.log_info(
            f'Executing query: "{query}" with parameters: {parameters}'
        )

        try:
            with self._driver.session() as session:
                response = session.run(query, parameters or {})
                result = [element.data() for element in response]
                self._logger.log_info(
                    f"Query executed successfully. Retrieved {len(result)} records."
                )
                return result
        except Exception as e:
            self._logger.log_error(f"Query execution failed: {e}")
            raise RuntimeError(f"Query execution failed: {e}")

    def _cast_to_nodes(self, result: List[Dict[str, Any]]) -> List[Node]:
        return [
            Node(labels=entry["labels"], properties=entry["props"]) for entry in result
        ]

    def get_nodes(
        self,
        labels: List[str] = None,
        properties: Dict[str, any] = None,
        limit: int = NEO4J_DEFAULT_NUMBER_OF_NODES,
    ) -> list[dict]:
        """Retrieve nodes with a specific labels."""
        if labels:
            query = f"MATCH (n:{':'.join(labels)})"
        else:
            query = "MATCH (n)"
        if properties:
            query += " WHERE " + " AND ".join(
                [f"n.{key} = {value}" for key, value in properties.items()]
            )
        query += " RETURN labels(n) AS labels, properties(n) AS props"
        query += f" LIMIT {limit}"
        self._logger.log_info(f"Retrieving nodes with labels: {labels}")
        result = self.execute_query(query)
        return self._cast_to_nodes(result) if result else []
