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
    
    _ = my_neo4j_driver.execute_query(
        query="MATCH (n) DETACH DELETE n",
        parameters=None,
    )

    # Create nodes
    trung = my_neo4j_driver.create_node(
        labels=[Label.PERSON], properties={"name": "Trung", "age": 25}
    )
    print(f"Created node: {trung}")
    huong = my_neo4j_driver.create_node(
        labels=[Label.PERSON], properties={"name": "Huong", "age": 26}
    )
    print(f"Created node: {huong}")
    phuong = my_neo4j_driver.create_node(
        labels=[Label.PERSON], properties={"name": "Phuong", "age": 26}
    )
    print(f"Created node: {phuong}")
    vo = my_neo4j_driver.create_node(
        labels=[Label.PERSON], properties={"name": "Vo", "age": 31}
    )
    print(f"Created node: {vo}")

    prison_break = my_neo4j_driver.create_node(
        labels=[Label.MOVIES],
        properties={"name": "Prison Break", "release_date": date(2005, 8, 29)},
    )
    print(f"Created node: {prison_break}")

    # Get all nodes
    nodes = my_neo4j_driver.get_nodes()
    print("All nodes in the database: ", nodes)

    # Update nodes
    trung_updated = my_neo4j_driver.update_node(
        labels=[Label.PERSON],
        match_criteria={"name": "Trung"},
        new_properties={"age": 999},
    )

    # Get all nodes again to see the update
    nodes = my_neo4j_driver.get_nodes()
    print("All nodes in the database: ", nodes)


if __name__ == "__main__":
    main()
