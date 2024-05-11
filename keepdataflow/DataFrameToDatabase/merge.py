import random
import string

import pandas as pd  # remove module from prod
from keepitsql import (
    CopyDDl,
    FromDataFrame,
)
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# self,
# table_name: str,
# dataframe: pd.DataFrame,
# match_columns: List[str] = None,
# upsert: bool = False,
# delete_requires: List[str] = None,
# include_metadata_timestamps: bool = None,


class merge:
    """Class for merging a dataframe into an SQL table."""

    def merge(
        self,
        dataframe: pd.DataFrame,  # cls or self
        target_table: str,
        database_module: str,
        match_condition: list,
        database_url: str,  # cls
        target_schema: str = None,
        batch_size: int = 1000,
        temp_db_type=None,
    ):
        # Set table parmeters
        uid = "".join(random.choices(string.ascii_lowercase, k=4))  # nosec B311
        temp_name = f"_source_{target_table}_{uid}"
        drop_temp_table = text(f"DROP TABLE {temp_name}")

        # Generate DDL
        source_table, temp_table = CopyDDl(
            database_url=database_url, local_table_name=target_table, local_schema_name=None
        ).create_ddl(
            new_table_name=temp_name, temp_dll_output='sqlite'
        )  ## change with temp table type

        table_name = target_table  ## place holder for table formater

        # Initialize temp table insert statement
        insert_temp = FromDataFrame(target_table=temp_name, target_schema=None)
        # Initialize merge statement
        merge_statment = FromDataFrame(target_table=table_name, target_schema=target_schema).set_df(
            new_dataframe=dataframe
        )
        with self.session as session:
            try:
                # Perform database operations
                # Phase 1: Create Temp Table
                create_temp_table = text(temp_table)
                session.execute(create_temp_table)

                # Phase 2: Insert Into Temp
                for start in range(0, len(dataframe), batch_size):
                    batch_data = dataframe[start : start + batch_size]

                    insert_sql = text(insert_temp.set_df(batch_data).sql_insert())
                    session.execute(insert_sql)

                # Phase 3: Execute Merge Statement
                merge_sql = merge_statment.sql_merge(source_table=temp_name, join_keys=match_condition)
                print(merge_sql)  # not ready for testing

                # Phase 4: Drop temp table
                session.execute(drop_temp_table)
                # Phase 5: Commit Datbase
                session.commit()
            except SQLAlchemyError as e:
                # The session is rolled back by the context manager, but you can handle errors specifically if needed
                print(f"An error occurred: {e}")
                raise
