from typing import Any, Dict, List
from neo4j import GraphDatabase
from datetime import date, datetime
from neo4j.time import Date as Neo4jDate, DateTime as Neo4jDateTime

from logger.logger import Logger
from models.neo4j_driver_models.connection_model import ConnectionModel
from models.neo4j_driver_models.database_models import Node, Relationship
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
            Node(
                id=entry["id"],
                labels=entry["labels"],
                properties=convert_props(entry["properties"]),
            )
            for entry in result
        ]

    def _cast_to_relationships(
        self, result: List[Dict[str, Any]]
    ) -> List[Relationship]:
        def convert_value(value):
            if isinstance(value, Neo4jDate):
                return date(value.year, value.month, value.day)
            elif isinstance(value, Neo4jDateTime):
                return datetime(
                    value.year,
                    value.month,
                    value.day,
                    value.hour,
                    value.minute,
                    value.second,
                    value.microsecond,
                )
            return value

        def convert_props(props):
            return {k: convert_value(v) for k, v in props.items()}

        return [
            Relationship(
                id=entry["id"],
                start_id=entry["start_id"],
                end_id=entry["end_id"],
                type=entry["type"],
                properties=convert_props(entry["properties"]),
            )
            for entry in result
        ]

    def get_nodes(
        self,
        labels: List[Label] = None,
        properties: Dict[str, any] = None,
        limit: int = NEO4J_DEFAULT_NUMBER_OF_NODES,
    ) -> list[Node]:
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
        query += " RETURN id(n) AS id, labels(n) AS labels, properties(n) AS properties"
        query += f" LIMIT {limit}"
        self._logger.log_info(
            f"Retrieving nodes with labels: {[label.value for label in labels] if labels else '*'} "
        )
        result = self.execute_query(query)
        return self._cast_to_nodes(result) if result else []

    def create_node(self, labels: List[Label], properties: Dict[str, Any]) -> Node:
        """Create a new node in the Neo4j database."""
        query = f"CREATE (n:{':'.join([label.value for label in labels])}) SET n = $properties RETURN id(n) AS id, labels(n) AS labels, properties(n) AS properties"
        parameters = {"properties": properties}
        result = self.execute_query(query, parameters)
        return self._cast_to_nodes(result)[0] if result else None

    def update_nodes(
        self,
        labels: List[Label] = None,
        match_criteria: Dict[str, Any] = None,
        new_properties: Dict[str, Any] = None,
    ) -> Node:
        """Update an existing node in the Neo4j database."""
        if not new_properties:
            self._logger.log_error("New properties must be provided for update.")
            raise ValueError("New properties must be provided for update.")

        match_clause = "MATCH (n"
        if labels:
            match_clause += f":{':'.join(label.value for label in labels)}"
        match_clause += ")"

        where_clause = ""
        if match_criteria:
            conditions = [f"n.{key} = ${key}" for key in match_criteria]
            where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
        {match_clause}
        {where_clause}
        SET n += $new_properties
        RETURN id(n) AS id, labels(n) AS labels, properties(n) AS properties
        """

        parameters = {**(match_criteria or {}), "new_properties": new_properties}

        self._logger.log_info(
            f"Updating node with labels: {[label.value for label in labels] if labels else '*'} "
            f"and match_criteria: {match_criteria or '{}'}, new_properties: {new_properties}"
        )

        result = self.execute_query(query, parameters)
        return self._cast_to_nodes(result)[0] if result else None

    def get_relationships(
        self,
        types: List[RelationshipType] = None,
        start_node_labels: List[Label] = None,
        start_node_properties: Dict[str, Any] = None,
        end_node_labels: List[Label] = None,
        end_node_properties: Dict[str, Any] = None,
        limit: int = NEO4J_DEFAULT_NUMBER_OF_NODES,
    ) -> List[Relationship]:
        """Retrieve relationships between nodes."""
        start_node_str = "start_node"
        if start_node_labels:
            start_node_str += (
                f":{':'.join([label.value for label in start_node_labels])}"
            )
        if start_node_properties:
            start_node_str += " WHERE " + " AND ".join(
                [
                    (
                        f"start_node.{key} = '{value}'"
                        if isinstance(value, str)
                        else f"start_node.{key} = {value}"
                    )
                    for key, value in start_node_properties.items()
                ]
            )

        end_node_str = "end_node"
        if end_node_labels:
            end_node_str += f":{':'.join([label.value for label in end_node_labels])}"
        if end_node_properties:
            end_node_str += " WHERE " + " AND ".join(
                [
                    (
                        f"end_node.{key} = '{value}'"
                        if isinstance(value, str)
                        else f"end_node.{key} = {value}"
                    )
                    for key, value in end_node_properties.items()
                ]
            )

        if types:
            type_str = ":" + ":".join(rt.value for rt in types)
        else:
            type_str = ""

        query = (
            f"MATCH ({start_node_str})-[r{type_str}]->({end_node_str}) "
            "RETURN id(r) as id, id(start_node) as start_id, id(end_node) as end_id, type(r) AS type, properties(r) AS properties "
            f"LIMIT {limit}"
        )

        result = self.execute_query(query)
        return self._cast_to_relationships(result) if result else None

    def create_relationship(
        self,
        start_node_labels: List[Label],
        start_node_properties: Dict[str, Any],
        end_node_labels: List[Label],
        end_node_properties: Dict[str, Any],
        type: RelationshipType,
        properties: Dict[str, Any] = None,
    ) -> Relationship:
        """Create a relationship between two nodes."""

        query = (
            f"MATCH (start:{':'.join([label.value for label in start_node_labels])} "
            f"{{{', '.join([f'{k}: $start_{k}' for k in start_node_properties])}}}), "
            f"(end:{':'.join([label.value for label in end_node_labels])} "
            f"{{{', '.join([f'{k}: $end_{k}' for k in end_node_properties])}}}) "
            f"CREATE (start)-[r:{type.value} $props]->(end) "
            f"RETURN id(r) AS id, id(start) AS start_id, id(end) AS end_id, type(r) AS type, properties(r) AS properties"
        )

        parameters = {
            **{f"start_{k}": v for k, v in start_node_properties.items()},
            **{f"end_{k}": v for k, v in end_node_properties.items()},
            "props": properties or {},
        }

        result = self.execute_query(query, parameters)
        return self._cast_to_relationships(result)[0] if result else None

    def update_relationships(
        self,
        start_node_labels: List[Label] = None,
        start_node_properties: Dict[str, Any] = None,
        end_node_labels: List[Label] = None,
        end_node_properties: Dict[str, Any] = None,
        relationship_type: RelationshipType = None,
        new_properties: Dict[str, Any] = None,
    ) -> Relationship:
        """Update an existing relationship with new properties."""

        if not relationship_type:
            self._logger.log_error("Relationship type must be provided.")
            raise ValueError("Relationship type must be provided.")

        if not new_properties:
            self._logger.log_error("New properties must be provided for update.")
            raise ValueError("New properties must be provided for update.")

        # Build MATCH pattern for start node
        start_node_pattern = "start"
        if start_node_labels:
            start_node_pattern += (
                f":{':'.join(label.value for label in start_node_labels)}"
            )
        if start_node_properties:
            start_node_pattern += (
                " {"
                + ", ".join(f"{k}: $start_{k}" for k in start_node_properties)
                + "}"
            )

        # Build MATCH pattern for end node
        end_node_pattern = "end"
        if end_node_labels:
            end_node_pattern += f":{':'.join(label.value for label in end_node_labels)}"
        if end_node_properties:
            end_node_pattern += (
                " {" + ", ".join(f"{k}: $end_{k}" for k in end_node_properties) + "}"
            )

        match_clause = f"MATCH ({start_node_pattern})-[r:{relationship_type.value}]->({end_node_pattern})"

        query = (
            f"{match_clause} SET r += $new_properties "
            "RETURN id(r) AS id, id(start) AS start_id, id(end) AS end_id, type(r) AS type, properties(r) AS properties"
        )

        parameters = {
            **{f"start_{k}": v for k, v in (start_node_properties or {}).items()},
            **{f"end_{k}": v for k, v in (end_node_properties or {}).items()},
            "new_properties": new_properties,
        }

        self._logger.log_info(
            f"Updating relationship of type '{relationship_type.value}' with new properties: {new_properties}"
        )

        result = self.execute_query(query, parameters)
        return self._cast_to_relationships(result) if result else None

    def delete_relationship(
        self,
        start_node_labels: List[Label],
        start_node_properties: Dict[str, Any],
        end_node_labels: List[Label],
        end_node_properties: Dict[str, Any],
        relationship_type: RelationshipType,
        new_properties: Dict[str, Any],
    ) -> Relationship:
        """Update an existing relationship."""
        match_clause = (
            f"MATCH (start:{':'.join([label.value for label in start_node_labels])} "
            f"{{{', '.join([f'{k}: $start_{k}' for k in start_node_properties])}}})-"
            f"[r:{relationship_type.value}]->(end:{':'.join([label.value for label in end_node_labels])} "
            f"{{{', '.join([f'{k}: $end_{k}' for k in end_node_properties])}}})"
        )

        query = (
            f"{match_clause} SET r += $new_properties "
            "RETURN id(r) AS id, id(start) AS start_id, id(end) AS end_id, type(r) AS type, properties(r) AS properties"
        )

        parameters = {
            **{f"start_{k}": v for k, v in start_node_properties.items()},
            **{f"end_{k}": v for k, v in end_node_properties.items()},
            "new_properties": new_properties,
        }

        result = self.execute_query(query, parameters)
        return self._cast_to_relationships(result)[0] if result else None
