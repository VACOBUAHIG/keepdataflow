import sqlite3
from typing import (
    Any,
    Optional,
)

import pandas as pd
from sqlalchemy import (
    URL,
    create_engine,
    text,
)

# from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from keepdataflow.core.database_to_database import DatabaseToDatabase
from keepdataflow.core.dataframe_to_db import DataframeToDatabase


class SqlConn:
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

    def __init__(self, database_url: str) -> None:
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
        self.df_to_db = DataframeToDatabase(self.database_url, self.db_engine, self.Session)
        self.db_to_db = DatabaseToDatabase(self.df_to_db)

    def __getattr__(self, name: str) -> Any:
        """
        Delegate attribute access to df_to_db or db_to_db instance.

        This method is called when an attribute is not found in the SqlConn instance.
        It first tries to find the attribute in the db_to_db instance, then in the df_to_db instance.

        If accessing a method on df_to_db, it ensures that load_df has been called first.

        Parameters:
            - name (str): The name of the attribute to access.

        Returns:
            Any: The attribute from the db_to_db or df_to_db instance if found.
        """
        if hasattr(self.db_to_db, name):
            return getattr(self.db_to_db, name)
        elif hasattr(self.df_to_db, name):
            attr = getattr(self.df_to_db, name)
            if callable(attr):

                def wrapper(*args: Any, **kwargs: Any) -> Any:
                    if not hasattr(self.df_to_db, 'source_dataframe') or self.df_to_db.source_dataframe is None:
                        raise AttributeError(f"DataFrame must be loaded using 'load_df' before calling '{name}'")
                    return attr(*args, **kwargs)

                return wrapper
            return attr
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    # def db_to_db(self, source_db_url: str, source_table_name: str, source_schema: str = None) -> None:
    #     source_engine = create_engine(source_db_url)
    #     source_session = sessionmaker(bind=source_engine)

    # # Read data from the source table into a DataFrame
    #     source_data = pd.read_sql_table(schema=source_schema,table_name=source_table_name,con=source_engine)

    # Use the DataframeToDatabase instance to refresh data in the target database
    # self.df_to_db.merge_data(
    #     source_dataframe=source_data,
    #     target_table=source_table_name,
    #     target_schema =source_schema,
    #     match_condition=None,
    #     dbms_type='sqlite',
    # )
    # def ddd(DataframeToDatabase):
    #     pass

    # .refresh_data(
    #     source_dataframe=source_data,
    #     target_table=source_table_name,
    #     database_module=self.db_engine,
    #     target_schema=target_schema,
    # )
    # source_session.close()

    # def df_to_db(self):
    #     return DataframeToDatabase(self.database_url,self.db_engine,self.Session)

    # def df_to_db(self):
    #     self.DataframeToDatabase


# class DataframeToDatabase:
#     def __init__(self, database_url: str) -> None:
#         self.database_url = database_url
#         self.db_engine = create_engine(self.database_url)
#         self.Session = sessionmaker(bind=self.db_engine)


#     def df_to_db(self) -> None:
#         DataframeToDatabase()


# self.df_to_db = example_function(arg1=self._default1, arg2=self._default2, arg3=None)


data = {
    "ItemID": ["ID101", "ID102", "ID103", "ID104"],
    "ItemName": ["Laptop", "Desk Chair", "USB-C Cable", "Monitor"],
    "Description": [
        "15-inch laptop with 8GB RAM",
        "Ergonomic office chair",
        "1m USB-C charging cable",
        "24-inch LED monitors",
    ],
    "Category": ["Electronics", "Furniture", "Electronics", "Electronics"],
    "Quantity": [10, 5, 50, 8],
    "Location": ["Warehouse A", "Warehouse F", "Warehouse A", "Warehouse C"],
}
df = pd.DataFrame(data)
sql_db2 = "sqlite:////Users/themobilescientist/Documents/projects/archive/keepitsql/test.db"

sql_db3 = "sqlite:////Users/themobilescientist/Documents/projects/archive/keepitsql/source_test.db"


get_conn = SqlConn(sql_db2)
get_conn.df_to_db.load_df(df).merge_data(
    target_table="human",
    match_condition=['ItemID'],
    dbms_type='sqlite',
)

get_conn.db_to_db.copy_source_db(sql_db3, 'human').merge_data(
    target_table="human",
    match_condition=['ItemID'],
    dbms_type='sqlite',
)


# get_conn.db_to_db.copy_source_db(sql_db3, 'human').merge_data(
#     target_table="human",
#     match_condition=['ItemID'],
#     dbms_type='sqlite',
# )
