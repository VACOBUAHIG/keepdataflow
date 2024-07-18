from typing import (
    Any,
    Optional,
    Union,
)

from keepdataflow._database_engine import DatabaseEngine
from keepdataflow.database_operations import DatabaseOperations


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

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.database_engine = DatabaseEngine(database_url)  # Integrate DatabaseEngine
        self.operations = DatabaseOperations(self.database_engine)

    def from_dataframe(self, dataframe):
        return self.operations.load_dataframe(dataframe)

    def from_database(
        self,
        source_db_url: str,
        source_table_name: Optional[str] = None,  # Fully qualified
        source_query: Optional[Union[str, bytes]] = None,
        chunk_size: Optional[int] = None,
        **kwargs,
    ):
        return self.operations.copy_source_db(source_db_url, source_table_name, source_query, chunk_size)
