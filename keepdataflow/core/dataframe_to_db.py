import random
import string
from typing import (
    Any,
    List,
    Optional,
)

from keepitsql import (
    CopyDDl,
    FromDataframe,
    get_table_column_info,
)
from pandas import DataFrame
from sqlalchemy import (
    create_engine,
    text,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import sessionmaker

from .conversion import read_sql_table_and_convert_types

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


def generate_database_connection(database_url: str, py_database_module: Any) -> Any:
    """
    Generates a database connection.

    Args:
        database_url (str): The URL of the database.
        py_database_module (Any): The database module to use, e.g., psycopg2, sqlite3, etc.

    Returns:
        Any: The database connection object.
    """
    db_url = database_url
    connection = py_database_module.connect(db_url)
    return connection


class DataframeToDatabase:
    def __init__(self, database_url: str, db_engine: Optional[Any] = None, session: Optional[Any] = None) -> None:
        """
        Initializes the DataframeToDatabase instance.

        Args:
            database_url (str): The URL of the database.
            db_engine (Optional[Any]): The database engine instance. Default is None.
            session (Optional[Any]): The session instance. Default is None.
        """
        self.database_url = database_url
        self.db_engine = create_engine(self.database_url) if db_engine is None else db_engine
        self.Session = sessionmaker(bind=self.db_engine) if session is None else session
        self.source_dataframe: Optional[DataFrame] = None

    def load_df(self, source_dataframe: Any) -> 'DataframeToDatabase':
        """
        Loads the source DataFrame into the instance.

        Args:
            source_dataframe (DataFrame): The source DataFrame.

        Returns:
            DataframeToDatabase: The instance with the loaded DataFrame.
        """
        self.source_dataframe = source_dataframe
        return self

    def refresh_data(
        self,
        target_table: str,
        database_module: Any,
        target_schema: Optional[str] = None,
        batch_size: int = 1000,
        select_column: Optional[List[str]] = None,
        temp_type: Optional[str] = None,
    ) -> None:
        """
        Refreshes data in a target table by truncating and reloading from a source DataFrame.

        Args:
            target_table (str): The target table name.
            database_module (Any): The database module to use, e.g., psycopg2, sqlite3, etc.
            target_schema (Optional[str]): The target schema, if applicable. Default is None.
            batch_size (int): The batch size for inserting data. Default is 1000.
            select_column (Optional[List[str]]): List of columns to select. Default is None.
            temp_type (Optional[str]): Temporary table type, if applicable. Default is None.

        Returns:
            None
        """
        if self.source_dataframe is None:
            raise ValueError("Source DataFrame is not loaded. Please load the DataFrame using `load_df` method.")

        truncate_table = f"DELETE FROM {target_table}"

        connection = generate_database_connection(database_url=self.database_url, py_database_module=database_module)
        cursor = connection.cursor()
        load_df = FromDataframe(target_table="human", target_schema=None)

        try:
            cursor.execute(truncate_table)

            for start in range(0, len(self.source_dataframe), batch_size):
                batch_data = self.source_dataframe[start : start + batch_size]
                insert_statement = load_df.set_df(batch_data).sql_insert(
                    column_select=select_column, temp_type=temp_type
                )
                cursor.execute(insert_statement)
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
        target_table: str,
        target_schema: Optional[str] = None,
        match_condition: Optional[List[str]] = None,
        constraint_columns: Optional[List[str]] = None,
        batch_size: int = 1000,
    ) -> None:
        """
        Merges data from a source DataFrame into a target table.

        Args:
            target_table (str): The target table name.
            dbms_type (str): The type of the database management system (e.g., 'mssql').
            match_condition (List[str]): List of conditions to match records. Default is None.
            constraint_columns (Optional[List[str]]): List of constraint columns. Default is None.
            target_schema (Optional[str]): The target schema, if applicable. Default is None.
            batch_size (int): The batch size for inserting data. Default is 1000.

        Returns:
            None
        """
        if self.source_dataframe is None:
            raise ValueError("Source DataFrame is not loaded. Please load the DataFrame using `load_df` method.")

        uid = "".join(random.choices(string.ascii_lowercase, k=4))
        temp_name = f"_source_{target_table}_{uid}"

        # dbms_dialect = self.db_engine.dialect.name
        dbms_dialect = self.Session().bind.dialect.name
        if dbms_dialect == 'mssql':
            temp_target_name = f'##{temp_name}'
        else:
            temp_target_name = temp_name

        drop_temp_table = text(f"DROP TABLE {temp_target_name}")

        source_table, temp_table = CopyDDl(
            database_url=self.database_url, local_table_name=target_table, local_schema_name=target_schema
        ).create_ddl(new_table_name=temp_name, temp_dll_output=dbms_dialect, drop_primary_key='N')

        for column in self.source_dataframe.select_dtypes(include=['float', 'object']):
            self.source_dataframe[column] = self.source_dataframe[column].apply(lambda x: None if x == "" else x)

        with self.Session() as session:
            # self.source_dataframe = read_sql_table_and_convert_types(
            #     self.source_dataframe, session, table_name=target_table, schema_name=target_schema
            # )

            try:
                auto_columns, primary_key_list = get_table_column_info(session, target_table, target_schema)

                constraint_columns = auto_columns if constraint_columns is None else constraint_columns
                match_condition = primary_key_list if match_condition is None else match_condition

                create_temp_table = text(temp_table)
                session.execute(create_temp_table)
                print("Temp table create complete")
                connection = session.connection()

                insert_conn = FromDataframe(
                    target_table=temp_target_name, target_schema=None, dataframe=self.source_dataframe
                )
                insert_sql = insert_conn.insert()

                for start in range(0, len(self.source_dataframe), batch_size):
                    batch_data = self.source_dataframe[start : start + batch_size]

                    params_list = batch_data.to_dict(orient='records')

                    session.execute(text(insert_sql), params_list)

                print("Temp table load complete")

                merge_sql = FromDataframe(
                    target_table=target_table, target_schema=target_schema, dataframe=self.source_dataframe
                ).upsert(
                    source_table=temp_target_name,
                    match_condition=match_condition,
                    dbms_output=dbms_dialect,
                    constraint_columns=constraint_columns,
                )

                ### Need to review for getting the right statmen
                upsert_statement = text(merge_sql)
                if dbms_dialect == 'sqlite':
                    session.execute(upsert_statement, params_list)
                else:
                    session.execute(upsert_statement)

                print("Upsert Complete")

                session.execute(drop_temp_table)
                session.commit()
                print("Insert Complete")
            except SQLAlchemyError as e:
                session.rollback()
                print(f"An error occurred: {e}")
                raise
            finally:
                session.close()
