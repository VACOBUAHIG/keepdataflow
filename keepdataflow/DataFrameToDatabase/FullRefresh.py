import random
import string

import pandas as pd  # remove module from prod
from keepitsql import (
    CopyDDl,
    FromDataframe,
)
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError


class FullRefresh:
    def full_refresh(
        self,
        dataframe: pd.DataFrame,  # cls or self
        target_table: str,
        target_schema: str = None,
        batch_size: int = 1000,
        select_column: list = None,
    ) -> None:
        """Refreshes data in a target table by truncating and reloading from a source DataFrame.

        Args:
            source_dataframe (pd.DataFrame): The source data.
            target_table (str): The target table name.
            database_url (str): The database connection URL.
            database_module (Any): The database module to use, e.g., psycopg2, sqlite3, etc.
            target_schema (Optional[str]): The target schema, if applicable. Default is None.
            batch_size (int): The batch size for inserting data. Default is 1000.
        """

        target_tbl = target_table  # code to add to schmea
        truncate_table = text(f"DELETE FROM {target_tbl}")

        # # insert_statment = FromDataFrame(source_dataframe=source_dataframe,target_table=target_tbl).sql_insert()
        # # shouild i initialized connection in in  sepearte function ?
        # connection = generate_datbase_connection(
        #     database_url=self.database_url, py_database_module=database_module
        # )  # cls
        # # connection.autocommit = False  # Important for transaction management
        # cursor = connection.cursor()  # cls
        # load_df = FromDataFrame(target_table="human", target_schema=None)  # cls

        # Initialize temp table insert statement
        insert_temp = FromDataframe(target_table=target_table, target_schema=target_schema)

        with self.session as session:
            try:
                # Perform database operations
                # Phase 1: Truncate Table
                session.execute(truncate_table)

                # Phase 2: Insert Into Table
                for start in range(0, len(dataframe), batch_size):
                    batch_data = dataframe[start : start + batch_size]

                    insert_sql = text(insert_temp.set_df(batch_data).sql_insert())
                    session.execute(insert_sql)
                # Phase 5: Commit Datbase
                session.commit()
            except SQLAlchemyError as e:
                # The session is rolled back by the context manager, but you can handle errors specifically if needed
                print(f"An error occurred: {e}")
                raise
