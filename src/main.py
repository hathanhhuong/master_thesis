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
    uyen = my_neo4j_driver.create_node(
        labels=[Label.PERSON], properties={"name": "Uyen", "age": 28}
    )
    print(f"Created node: {uyen}")

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
    print(f"Updated node: {trung_updated}")

    # Get all nodes again to see the update
    nodes = my_neo4j_driver.get_nodes()
    print("All nodes in the database: ", nodes)

    # Create relationships
    vo_knows_huong = my_neo4j_driver.create_relationship(
        start_node_labels=[Label.PERSON],
        start_node_properties={"name": "Vo"},
        end_node_labels=[Label.PERSON],
        end_node_properties={"name": "Huong"},
        type=RelationshipType.KNOWS,
        properties={"since": date(2023, 1, 1)},
    )
    print(f"Created relationship: {vo_knows_huong}")

    huong_knows_vo = my_neo4j_driver.create_relationship(
        start_node_labels=[Label.PERSON],
        start_node_properties={"name": "Huong"},
        end_node_labels=[Label.PERSON],
        end_node_properties={"name": "Vo"},
        type=RelationshipType.KNOWS,
        properties={"since": date(2023, 1, 1)},
    )
    print(f"Created relationship: {huong_knows_vo}")

    trung_watches_prison_break = my_neo4j_driver.create_relationship(
        start_node_labels=[Label.PERSON],
        start_node_properties={"name": "Trung"},
        end_node_labels=[Label.MOVIES],
        end_node_properties={"name": "Prison Break"},
        type=RelationshipType.WATCHES,
        properties={"since": date(2025, 6, 15)},
    )
    print(f"Created relationship: {trung_watches_prison_break}")

    # Get all relationships with start node properties
    all_relationships = my_neo4j_driver.get_relationships(
        start_node_properties={"name": "Huong"},
    )
    print("All relationships in the database: ", all_relationships)

    # Update relationships
    huong_knows_vo_updated = my_neo4j_driver.update_relationship(
        start_node_labels=[Label.PERSON],
        start_node_properties={"name": "Huong"},
        end_node_labels=[Label.PERSON],
        end_node_properties={"name": "Vo"},
        relationship_type=RelationshipType.KNOWS,
        new_properties={"since": date(2023, 2, 2)},
    )
    print(f"Updated relationship: {huong_knows_vo_updated}")

    # Get all relationships again to see the update
    all_relationships = my_neo4j_driver.get_relationships()
    print("All relationships in the database after update: ", all_relationships)


if __name__ == "__main__":
    main()
