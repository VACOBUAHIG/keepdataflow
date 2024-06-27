import sqlite3
from threading import Lock
from typing import (
    Any,
    Callable,
)

import pandas as pd
from sqlalchemy import (
    create_engine,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import (
    Session,
    sessionmaker,
)

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
        >>> session = sql_conn_instance.get_session()
    """

    _engine = None
    _lock = Lock()

    def __init__(self, database_url: str) -> None:
        """
        Constructor for the SqlConn class that takes a database URL and sets up
        the database engine and session maker.

        Establishes the internal engine to interact with the specified SQL database and prepares a sessionmaker to create sessions for executing transactions.

        Parameters:
            - database_url (str): The URL of the database to initialize a connection.
        """
        self.database_url = database_url
        self.db_engine = self._get_engine()
        self.Session = sessionmaker(bind=self.db_engine)
        self.df_to_db = DataframeToDatabase(self.database_url, self.db_engine, self.Session)
        self.db_to_db = DatabaseToDatabase(self.df_to_db)

    def _get_engine(self) -> Engine:
        """
        Get the singleton database engine instance, creating it if it does not exist.

        Returns:
            Engine: The SQLAlchemy engine instance.
        """
        if SqlConn._engine is None:
            with SqlConn._lock:
                if SqlConn._engine is None:  # Double-checked locking
                    SqlConn._engine = create_engine(self.database_url)
        return SqlConn._engine

    @property
    def get_session(self) -> Session:
        """
        Creates and returns a new SQLAlchemy session.

        Returns:
            Session: A new SQLAlchemy session.
        """
        return self.Session()

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
