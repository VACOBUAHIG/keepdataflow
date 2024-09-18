from typing import (
    Any,
    Optional,
    Union,
)

import pandas as pd
import polars as pl
from sqlalchemy import create_engine

# from keepdataflow.core.dataframe_to_db import DataframeToDatabase


class DatabaseToDatabase:
    def copy_source_db(
        # self,
        source_db_url: str,
        source_table_name: Optional[str] = None,  # Fully qualified
        source_query: Optional[Union[str, bytes]] = None,
        chunk_size: Optional[int] = None,
        # partitions: Optional[int] = 2,
    ) -> None:
        """
        Copies data from a source database table or SQL query/SQL file to the target database using Dask for parallel processing.

        Args:
            source_db_url (str): The URL of the source database.
            source_table_name (Optional[str]): The name of the source table. Default is None.
            source_schema (Optional[str]): The schema of the source table. Default is None.
            source_query (Optional[Union[str, bytes]]): SQL query string or path to an SQL file. Default is None.
            batch_size (Optional[int]): Number of rows per batch when fetching data. Default is 1000.

        Returns:
            Any: The result of the df_to_db.load_df method, which handles loading the dataframe to the target database.
        """
        source_engine = create_engine(source_db_url)

        if not any([source_table_name, source_query]):
            raise ValueError("Either source_table_name or source_query must be provided.")

        query = f"SELECT * FROM {source_table_name}"
        sql_query = source_query if source_query is not None else query

        # Read the table into a Pandas DataFrame
        with source_engine.connect() as connection:
            df = pl.read_database_uri(sql_query, source_db_url, engine="connectorx", partition_range=chunk_size)
            # df = pl.from_pandas(pd.read_sql(sql_query, connection))
        return df


db = "sqlite:////Users/themobilescientist/Documents/projects/archive/keepitsql/test.db"

sql = 'Select * From human'


get_db = DatabaseToDatabase
tpin = get_db.copy_source_db(source_table_name='human', source_db_url=db, chunk_size=6000)
print(tpin)
