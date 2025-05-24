from neo4j import GraphDatabase

from logger.logger import Logger
from models.neo4j_driver_models.connection_model import ConnectionModel


class Neo4jDriver:
    def __init__(self, logger: Logger):
        self._logger = logger
        self._connection_model: ConnectionModel = None
        self._driver = None
        self._session = None

    def _test_connection(self) -> bool:
        """Test the connection to the Neo4j database."""
        try:
            with self._session as session:
                _ = session.run("RETURN 1")
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
            self._session = self._driver.session()

            if not self._test_connection():
                raise ValueError("Connection test failed.")

            self._logger.log_info("Successfully connected to Neo4j database.")

        except Exception as e:
            self._logger.log_error(f"Failed to connect to Neo4j: {e}")

    def close(self):
        self._driver.close()

    # def test(self) -> None:
    #     query = "MATCH (n) RETURN n"
    #     result = self._driver.execute_query(query)
    #     print(result)

    # def execute_query(self, query: str, parameters=None):
    #     with self._driver.session() as session:
    #         result = session.run(query, parameters or {})
    #         return [record.data() for record in result]
