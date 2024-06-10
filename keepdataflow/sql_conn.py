import sqlite3

import pandas as pd
from sqlalchemy import (
    URL,
    create_engine,
    text,
)

# from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from keepdataflow.core.dataframe_to_db import DataframeToDatabase


class SqlConn(DataframeToDatabase):
    """
    The SqlConn class creates a connection to a SQL database by extending the DataframeToDatabase class.

    This class is responsible for establishing and managing connections to the database using SQLAlchemy. It houses the functionality to create an engine that communicates with the database and to generate session objects for performing transactions.

    Attributes:
        - database_url (str): A string representing the URL of the database to which we intend to connect.
        - db_engine: An Engine object which is the core interface to the database, as provided by SQLAlchemy.
        - Session: A sessionmaker instance that generates new Session objects when called,
                   bound to the database engine.

    Methods:
        - __init__(self, database_url): The constructor initializes the database connection and session creation facilities.

    Example usage:
        >>> sql_conn_instance = SqlConn('sqlite:///example.db')
        >>> session = sql_conn_instance.Session()
    """

    def __init__(self, database_url) -> None:
        """
        Constructor for the SqlConn class that takes a database URL and sets up
        the database engine and session maker.

        Establishes the internal engine to interact with the specified SQL database and prepares a sessionmaker to create sessions for executing transactions.

        Parameters:
            - database_url (str): The URL of the database to initialize a connection.
        """
        self.database_url = database_url
        self.db_engine = create_engine(self.database_url)
        self.Session = sessionmaker(bind=self.db_engine)


# sql_db2 = "sqlite:////Users/themobilescientist/Documents/projects/archive/keepitsql/test.db"
# data = {
#     "ItemID": ["ID101", "ID102", "ID103", "ID104"],
#     "ItemName": ["Laptop", "Desk Chair", "USB-C Cable", "Monitor"],
#     "Description": [
#         "15-inch laptop with 8GB RAM",
#         "Ergonomic office chair",
#         "1m USB-C charging cable",
#         "24-inch LED monitors",
#     ],
#     "Category": ["Electronics", "Furniture", "Electronics", "Electronics"],
#     "Quantity": [10, 5, 50, 8],
#     "Location": ["Warehouse A", "Warehouse F", "Warehouse A", "Warehouse C"],
# }

# df = pd.DataFrame(data)

# sql_conn = SqlConn(sql_db2).merge_data(
#     source_dataframe=df,
#     target_table="human",
#     match_condition=['ItemID'],
#     dbms_type='sqlite',
# )
