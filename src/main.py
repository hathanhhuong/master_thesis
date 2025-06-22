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
    
    # Step 1: Create a test node
    print("Creating test node...")
    test_node = my_neo4j_driver.create_node(
        labels=[Label.PERSON], properties={"name": "John Doe", "age": 30}
    )
    print("Created node:", test_node)

    # Step 2: Update the test node's age and name
    print("Updating test node...")
    updated_node = my_neo4j_driver.update_node(
        labels=[Label.PERSON],
        match_criteria={"name": "John Doe"},  # Match using unique property
        new_properties={"age": 35, "name": "Johnathan Doe"},
    )
    print("Updated node:", updated_node)


if __name__ == "__main__":
    main()
