from typing import (
    Any,
    Optional,
    Union,
)

import pandas as pd
from sqlalchemy import create_engine

from keepdataflow.core.dataframe_to_db import DataframeToDatabase


class DatabaseToDatabase:
    """
    A class to facilitate the copying of data from one database to another.

    Attributes:
        df_to_db (DataframeToDatabase): An instance of DataframeToDatabase to handle loading dataframes to the database.
    """

    def __init__(self, df_to_db: Any = DataframeToDatabase) -> None:
        """
        Initializes the DatabaseToDatabase instance.

        Args:
            df_to_db (DataframeToDatabase): An instance of DataframeToDatabase to handle loading dataframes to the database.
        """
        self.df_to_db = df_to_db

    def copy_source_db(
        self,
        source_db_url: str,
        source_table_name: Optional[str] = None,
        source_schema: Optional[str] = None,
        source_query: Optional[Union[str, bytes]] = None,
    ) -> Any:
        """
        Copies data from a source database table or SQL query/SQL file to the target database using the DataframeToDatabase instance.

        Args:
            source_db_url (str): The URL of the source database.
            source_table_name (Optional[str]): The name of the source table. Default is None.
            source_schema (Optional[str]): The schema of the source table. Default is None.
            source_query (Optional[Union[str, bytes]]): SQL query string or path to an SQL file. Default is None.

        Returns:
            Any: The result of the df_to_db.load_df method, which handles loading the dataframe to the target database.
        """
        source_engine = create_engine(source_db_url)

        if source_query is not None:
            if isinstance(source_query, str):
                # Check if source_query is a file path ending with '.sql'
                if source_query.endswith('.sql'):
                    # Read SQL query from file
                    with open(source_query, 'r') as file:
                        sql_query = file.read()
                else:
                    # Assume source_query is a SQL query string
                    sql_query = source_query

                # Execute query and read data into a DataFrame
                source_data = pd.read_sql_query(sql_query, con=source_engine)
            else:
                raise TypeError("source_query must be a string (SQL query or file path ending with '.sql').")

            if source_table_name is not None or source_schema is not None:
                raise ValueError("Cannot specify both source_query and source_table_name/source_schema.")
        else:
            # Read data from the source table into a DataFrame
            if source_table_name is None:
                raise ValueError("Either source_table_name or source_query must be provided.")

            source_data = pd.read_sql_table(schema=source_schema, table_name=source_table_name, con=source_engine)

        return self.df_to_db.load_df(source_data)
