import random
import sqlite3  # # remove module from prod
import string

import pandas as pd  # remove module from prod
from keepitsql import (
    CopyDDl,
    FromDataframe,
)
from pandas import DataFrame
from sqlalchemy import (
    create_engine,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

python_sql_drivers_to_db_abbreviations = {
    "psycopg2": "postgresql",
    "psycopg2-binary": "postgresql",
    "asyncpg": "postgresql+asyncpg",
    "mysqlclient": "mysql+mysqldb",
    "PyMySQL": "mysql+pymysql",
    "aiomysql": "mysql+aiomysql",
    "Built-in SQLite": "sqlite",
    "aiosqlite": "sqlite+aiosqlite",
    "cx_Oracle": "oracle",
    "pyodbc": "mssql+pyodbc",
    "pymssql": "mssql+pymssql",
    "pyfirebirdsql": "firebird+pyfirebirdsql",
    "fdb": "firebird+fdb",
    "sqlalchemy-sybase": "sybase+pysybase",
    "ibm_db_sa": "ibm_db_sa",
    "ibm_db": "ibm_db_sa",
    "sqlanydb": "sqlanywhere",
}


def generate_datbase_connection(database_url: str, py_database_module: str):
    db_url = database_url
    connection = py_database_module.connect(db_url)

    return connection


def database_insert():
    pass


class DataframeToDatabase:
    # def __init__(self, database_url) -> None:
    #     self.database_url = database_url
    #     self.db_engine = create_engine(self.database_url)
    #     self.Session = sessionmaker(bind=self.db_engine)

    def refresh_data(
        self,
        source_dataframe: DataFrame,  # cls or self
        target_table: str,
        # database_url: str, #cls
        database_module: str,
        target_schema: str = None,
        batch_size: int = 1000,
        select_column: list = None,
        temp_type=None,
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

        target_tbl = target_table
        truncate_table = f"DELETE FROM {target_table}"

        # insert_statment = FromDataFrame(source_dataframe=source_dataframe,target_table=target_tbl).sql_insert()
        # shouild i initialized connection in in  sepearte function ?
        connection = generate_datbase_connection(
            database_url=self.database_url, py_database_module=database_module
        )  # cls
        # connection.autocommit = False  # Important for transaction management
        cursor = connection.cursor()  # cls
        load_df = FromDataframe(target_table="human", target_schema=None)  # cls

        try:
            cursor.execute(truncate_table)

            for start in range(0, len(source_dataframe), batch_size):
                batch_data = source_dataframe[start : start + batch_size]
                insert_statment = load_df.set_df(batch_data).sql_insert(
                    column_select=select_column, temp_type=temp_type
                )
                cursor.execute(insert_statment)
                connection.commit()
                print("Transaction committed.")

        except Exception as e:
            print("Error occurred, rolling back transaction:", e)
            connection.rollback()

        finally:
            cursor.close()
            connection.close()

    def merge_data(
        self,
        source_dataframe: DataFrame,  # cls or self
        target_table: str,
        dbms_type: str,
        match_condition: list,
        temp_db_type=None,
        target_schema: str = None,
        batch_size: int = 1000,
        # database_url: str, #cls
    ):
        # Set table parmeters
        uid = "".join(random.choices(string.ascii_lowercase, k=4))  # nosec B311
        temp_name = f"_source_{target_table}_{uid}"
        drop_temp_table = text(f"DROP TABLE {temp_name}")

        # Generate DDL
        source_table, temp_table = CopyDDl(
            database_url=self.database_url, local_table_name=target_table, local_schema_name=None
        ).create_ddl(new_table_name=temp_name, temp_dll_output=dbms_type, drop_primary_key='Y')

        table_name = target_table  ## place holder for table formater

        # # Initialize temp table insert statement
        # insert_temp = FromDataframe(target_table=temp_name, target_schema=None,dataframe=source_dataframe).insert()
        # print(insert_temp)
        # Initialize merge statement
        # merge_statment = FromDataframe(target_table=table_name, target_schema=target_schema).set_df(
        #     new_dataframe=source_dataframe
        # )
        with self.Session() as session:
            try:
                # Phase 0
                ##initialize the Keepit

                # Perform database operations

                # Phase 1: Create Temp Table
                create_temp_table = text(temp_table)
                session.execute(create_temp_table)
                print("Temp table create complete")

                # Phase 2: Insert Into Temp
                for start in range(0, len(source_dataframe), batch_size):
                    batch_data = source_dataframe[start : start + batch_size]
                    insert_temp = FromDataframe(
                        target_table=temp_name, target_schema=None, dataframe=batch_data
                    ).insert()

                    insert_sql = text(insert_temp)
                    session.execute(insert_sql)
                print("Temp table load complete")
                # Phase 3: Execute Merge Statement
                merge_sql = FromDataframe(
                    target_table=table_name, target_schema=target_schema, dataframe=source_dataframe
                ).upsert(source_table=temp_name, match_condition=match_condition, dbms_output=dbms_type)
                upsert_statement = text(merge_sql)
                print("Upsert Complete")

                session.execute(upsert_statement)
                # # rint(merge_sql)  # not ready for testing

                # Phase 4: Drop temp table
                session.execute(drop_temp_table)
                # Phase 5: Commit Datbase
                session.commit()
                print("Insert Complete")
            except SQLAlchemyError as e:
                # The session is rolled back by the context manager, but you can handle errors specifically if needed
                session.rollback()
                print(f"An error occurred: {e}")
                raise
            # finally:
            #     # Close the session
            #     session.close()


# sql_db2 = "sqlite:////Users/themobilescientist/Documents/projects/keepitsql/test.db"
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
