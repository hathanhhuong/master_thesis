import os
from dotenv import load_dotenv
from database_driver.neo4j_driver import Neo4jDriver

from logger.logger import Logger, LogType
from models.neo4j_driver_models.connection_model import ConnectionModel
from utils.constants import LOG_FILE_BASE

load_dotenv()


def main():
    my_logger = Logger(file_name=LOG_FILE_BASE)
    my_logger.log_info("START")
    my_neo4j_driver = Neo4jDriver(my_logger)
    my_connection_model = ConnectionModel(
        host=os.getenv("NEO4J_HOST"),
        user=os.getenv("NEO4J_USER"),
        password=os.getenv("NEO4J_PASSWORD"),
    )
    my_neo4j_driver.connect(my_connection_model)
    result = my_neo4j_driver.execute_query(
        "MATCH (n) RETURN n LIMIT 10", {"hello": "world"}
    )
    print(result)


if __name__ == "__main__":
    main()
