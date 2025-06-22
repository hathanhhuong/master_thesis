from datetime import date
import os
from typing import List
from dotenv import load_dotenv
from database_driver.neo4j_driver import Neo4jDriver

from logger.logger import Logger, LogType
from models.neo4j_driver_models.connection_model import ConnectionModel
from models.neo4j_driver_models.database_models import Node
from utils.constants import LOG_FILE_BASE
from utils.enums import Label, RelationshipType

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
    relationships = my_neo4j_driver.get_relationships(
        relationship_types=[RelationshipType.ACTED_IN],
        end_node_labels=[Label.MOVIE],
        end_node_properties={"title": "As Good as It Gets"},
        limit=5,
    )
    print(relationships)


if __name__ == "__main__":
    main()
