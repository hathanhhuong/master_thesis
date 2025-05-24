from datetime import date
import os
from typing import List
from dotenv import load_dotenv
from database_driver.neo4j_driver import Neo4jDriver

from logger.logger import Logger, LogType
from models.neo4j_driver_models.connection_model import ConnectionModel
from models.neo4j_driver_models.database_models import Node
from utils.constants import LOG_FILE_BASE
from utils.enums import Label

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
    my_neo4j_driver.create_node(
        labels=[Label.MOVIE.value],
        properties={
            "title": "Avartar",
            "year": 2009,
            "rating": 7.8,
            "eventDate": date(2025, 5, 24),
        },
    )
    result: List[Node] = my_neo4j_driver.get_nodes(
        labels=[Label.MOVIE.value], properties={"title": "Avartar"}
    )
    my_date = result[0].properties["eventDate"]
    print(type(my_date))


if __name__ == "__main__":
    main()
