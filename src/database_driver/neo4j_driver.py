from typing import Any, Dict, List
from neo4j import GraphDatabase
from datetime import date, datetime
from neo4j.time import Date as Neo4jDate, DateTime as Neo4jDateTime

from logger.logger import Logger
from models.neo4j_driver_models.connection_model import ConnectionModel
from models.neo4j_driver_models.database_models import Node
from utils.constants import NEO4J_DEFAULT_NUMBER_OF_NODES
from utils.enums import Label, RelationshipType


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
        def convert_value(value):
            if isinstance(value, Neo4jDate):
                # Convert Neo4j Date to Python date
                return date(value.year, value.month, value.day)
            elif isinstance(value, Neo4jDateTime):
                # Convert Neo4j DateTime to Python datetime
                return datetime(
                    value.year,
                    value.month,
                    value.day,
                    value.hour,
                    value.minute,
                    value.second,
                    value.microsecond,
                )
            return value  # Leave other types unchanged

        def convert_props(props):
            return {k: convert_value(v) for k, v in props.items()}

        return [
            Node(labels=entry["labels"], properties=convert_props(entry["props"]))
            for entry in result
        ]

    def get_nodes(
        self,
        labels: List[Label] = None,
        properties: Dict[str, any] = None,
        limit: int = NEO4J_DEFAULT_NUMBER_OF_NODES,
    ) -> list[dict]:
        """Retrieve nodes with a specific labels."""
        if labels:
            query = f"MATCH (n:{':'.join([label.value for label in labels])})"
        else:
            query = "MATCH (n)"
        if properties:
            query += " WHERE " + " AND ".join(
                [
                    (
                        f"n.{key} = '{value}'"
                        if isinstance(value, str)
                        else f"n.{key} = {value}"
                    )
                    for key, value in properties.items()
                ]
            )
        query += " RETURN labels(n) AS labels, properties(n) AS props"
        query += f" LIMIT {limit}"
        self._logger.log_info(
            f"Retrieving nodes with labels: {[label.value for label in labels] if labels else '*'} "
        )
        result = self.execute_query(query)
        return self._cast_to_nodes(result) if result else []

    def create_node(self, labels: List[Label], properties: Dict[str, Any]) -> Node:
        """Create a new node in the Neo4j database."""
        query = f"CREATE (n:{':'.join([label.value for label in labels])}) SET n = $properties RETURN labels(n) AS labels, properties(n) AS props"
        parameters = {"properties": properties}
        result = self.execute_query(query, parameters)
        return self._cast_to_nodes(result)[0] if result else None

    def update_node(
        self,
        labels: List[Label],
        match_criteria: Dict[str, Any],
        new_properties: Dict[str, Any],
    ) -> Node:
        """Update an existing node in the Neo4j database."""
        match_clause = " AND ".join([f"n.{key} = ${key}" for key in match_criteria])
        query = f"""
        MATCH (n:{':'.join([label.value for label in labels])})
        WHERE {match_clause}
        SET n += $new_properties
        RETURN labels(n) AS labels, properties(n) AS props
        """
        parameters = {**match_criteria, "new_properties": new_properties}
        result = self.execute_query(query, parameters)
        return self._cast_to_nodes(result)[0] if result else None

    def get_relationships(
        self,
        relationship_types: List[RelationshipType],
        start_node_labels: List[Label] = None,
        start_node_properties: Dict[str, Any] = None,
        end_node_labels: List[Label] = None,
        end_node_properties: Dict[str, Any] = None,
        limit: int = NEO4J_DEFAULT_NUMBER_OF_NODES,
    ) -> List[Dict[str, Any]]:
        """Retrieve relationships between nodes."""
        start_node = "start_node"
        if start_node_labels:
            start_node += f":{':'.join([label.value for label in start_node_labels])}"
        if start_node_properties:
            start_node += " WHERE " + " AND ".join(
                [
                    (
                        f"start_node.{key} = '{value}'"
                        if isinstance(value, str)
                        else f"start_node.{key} = {value}"
                    )
                    for key, value in start_node_properties.items()
                ]
            )

        end_node = "end_node"
        if end_node_labels:
            end_node += f":{':'.join([label.value for label in end_node_labels])}"
        if end_node_properties:
            end_node += " WHERE " + " AND ".join(
                [
                    (
                        f"end_node.{key} = '{value}'"
                        if isinstance(value, str)
                        else f"end_node.{key} = {value}"
                    )
                    for key, value in end_node_properties.items()
                ]
            )

        query = (
            f"MATCH ({start_node})-[r:{':'.join([rt.value for rt in relationship_types])}]->({end_node}) "
            "RETURN start_node, end_node, type(r) AS relationship_type, properties(r) AS props "
            f"LIMIT {limit}"
        )

        result = self.execute_query(query)
        return result
