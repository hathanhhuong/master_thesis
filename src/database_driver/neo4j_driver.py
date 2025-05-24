from neo4j import GraphDatabase

from logger.logger import Logger
from models.neo4j_driver_models.connection_model import ConnectionModel


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
