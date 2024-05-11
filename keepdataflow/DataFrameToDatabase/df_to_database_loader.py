import random
import sqlite3  # # remove module from prod
import string

import pandas as pd  # remove module from prod
from keepitsql import (
    CopyDDl,
    FromDataFrame,
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
    def __init__(self, database_url) -> None:
        self.database_url = database_url
        self.db_engine = create_engine(self.database_url)
        self.Session = sessionmaker(bind=self.db_engine)

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
        load_df = FromDataFrame(target_table="human", target_schema=None)  # cls

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
        database_module: str,
        match_condition: list,
        target_schema: str = None,
        batch_size: int = 1000,
        temp_db_type=None,
        # database_url: str, #cls
    ):
        # Set table parmeters
        uid = "".join(random.choices(string.ascii_lowercase, k=4))  # nosec B311
        temp_name = f"_source_{target_table}_{uid}"
        drop_temp_table = text(f"DROP TABLE {temp_name}")

        # Generate DDL
        source_table, temp_table = CopyDDl(
            database_url=self.database_url, local_table_name=target_table, local_schema_name=None
        ).create_ddl(
            new_table_name=temp_name, temp_dll_output='sqlite'
        )  ## change with temp table type

        table_name = target_table  ## place holder for table formater

        # Initialize temp table insert statement
        insert_temp = FromDataFrame(target_table=temp_name, target_schema=None)
        # Initialize merge statement
        merge_statment = FromDataFrame(target_table=table_name, target_schema=target_schema).set_df(
            new_dataframe=source_dataframe
        )
        with self.Session() as session:
            try:
                # Perform database operations
                # Phase 1: Create Temp Table
                create_temp_table = text(temp_table)
                session.execute(create_temp_table)

                # Phase 2: Insert Into Temp
                for start in range(0, len(source_dataframe), batch_size):
                    batch_data = source_dataframe[start : start + batch_size]

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


sql_db2 = "sqlite:////Users/themobilescientist/Documents/projects/keepitsql/test.db"
data = {
    "ItemID": ["ID101", "ID102", "ID103", "ID104"],
    "ItemName": ["Laptop", "Desk Chair", "USB-C Cable", "Monitor"],
    "Description": [
        "15-inch laptop with 8GB RAM",
        "Ergonomic office chair",
        "1m USB-C charging cable",
        "24-inch LED monitor",
    ],
    "Category": ["Electronics", "Furniture", "Electronics", "Electronics"],
    "Quantity": [10, 5, 50, 8],
    "Location": ["Warehouse A", "Warehouse B", "Warehouse A", "Warehouse C"],
}


import polars as pl

df = pd.DataFrame(data)


# DataframeToDatabase(sql_db).refresh_data(
#     source_dataframe=df,
#     target_table="human",
#     database_module=sqlite3,
# )


sql_db2 = "sqlite:////Users/themobilescientist/Documents/projects/archive/keepitsql/test.db"

get_conn = DataframeToDatabase(sql_db2)


get_conn.merge_data(
    source_dataframe=df,
    target_table="human",
    match_condition=['ItemID'],
    database_module=sqlite3,
)


# poetry add git+https://github.com/geob3d/keepitsql.git

# def get_connection_type(database_url):
#     parsed_url = urlparse(database_url)
#     # The scheme component of the URL indicates the connection type
#     return parsed_url.scheme

# # Example Usage
# database_urls = [
#     "postgres://user:password@localhost:5432/sampledb",
#     "mysql://user:password@localhost:3306/sampledb",
#     "sqlite:///path/to/database.db",
# ]

# for url in database_urls:
#     print(f"URL: {url} - Connection Type: {get_connection_type(url)}")
