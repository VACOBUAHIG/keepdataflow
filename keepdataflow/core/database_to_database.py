from typing import (
    Any,
    Optional,
)

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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

    def copy_source_db(self, source_db_url: str, source_table_name: str, source_schema: Optional[str] = None) -> Any:
        """
        Copies data from a source database table to the target database using the DataframeToDatabase instance.

        Args:
            source_db_url (str): The URL of the source database.
            source_table_name (str): The name of the source table.
            source_schema (Optional[str]): The schema of the source table. Default is None.

        Returns:
            Any: The result of the df_to_db.load_df method, which handles loading the dataframe to the target database.
        """
        source_engine = create_engine(source_db_url)

        # Read data from the source table into a DataFrame
        source_data: pd.DataFrame = pd.read_sql_table(
            schema=source_schema, table_name=source_table_name, con=source_engine
        )

        return self.df_to_db.load_df(source_data)
