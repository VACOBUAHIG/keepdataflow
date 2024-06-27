import random
import string
from typing import (
    Any,
    List,
    Optional,
)

# from .conversion import read_sql_table_and_convert_types
import dask.dataframe as dd
from dask import delayed
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

# from dask import delayed, compute

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
        self.db_engine = db_engine
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
        # batch_size: int = 1000,
        partitions: int = 2,
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

        engine = self.db_engine
        if engine is None:
            raise ValueError("Database engine is not initialized")

        dbms_dialect = engine.dialect.name
        # dbms_dialect = self.Session().bind.dialect.name
        if dbms_dialect == 'mssql':
            temp_target_name = f'##{temp_name}'
        else:
            temp_target_name = temp_name

        drop_temp_table = text(f"DROP TABLE {temp_target_name}")

        ddf = dd.from_pandas(self.source_dataframe, npartitions=partitions)

        def create_temp_table() -> None:
            try:
                with engine.connect() as conn:
                    # with self.db_engine as session:
                    source_table, temp_table = CopyDDl(
                        database_url=self.database_url, local_table_name=target_table, local_schema_name=target_schema
                    ).create_ddl(new_table_name=temp_target_name, temp_dll_output=dbms_dialect, drop_primary_key='N')
                    create_temp_table = text(temp_table)
                    conn.execute(create_temp_table)
                conn.commit()
            except SQLAlchemyError as e:
                print(f"An error occurred while creating the temporary table: {e}")
                raise

        # Define function for batch insertion
        def execute_batch_insertion(batch_df: Any) -> None:
            """
            Execute batch insertion of a dataframe into a temporary table.

            Args:
                batch_df (Any): The dataframe to be inserted. It is expected to be a pandas DataFrame.

            Raises:
                SQLAlchemyError: If an error occurs during the insertion process.
            """
            Session = sessionmaker(bind=engine)
            with Session() as session:
                params_list = batch_df.to_dict(orient='records')
                insert_conn = FromDataframe(target_table=temp_target_name, target_schema=None, dataframe=batch_df)
                print(insert_conn)
                insert_sql = insert_conn.insert()
                session.execute(text(insert_sql), params_list)
                try:
                    print(insert_sql)
                    session.execute(text(insert_sql), params_list)
                    session.commit()
                except SQLAlchemyError as e:
                    session.rollback()
                    print(f"An error occurred during batch insertion: {e}")
                    raise

        def batch_insert():
            delayed_execute_batch_insertion = delayed(execute_batch_insertion)
            result = ddf.map_partitions(delayed_execute_batch_insertion, db_url=self.database_url)
            result.compute()

        def execute_merge():
            Session = sessionmaker(bind=engine)
            with Session() as session:
                try:
                    auto_columns, primary_key_list = get_table_column_info(
                        self.database_url, target_table, target_schema
                    )

                    constraint_list = auto_columns if constraint_columns is None else constraint_columns
                    match_list = primary_key_list if match_condition is None else match_condition

                    # Perform the final merge operation
                    merge_sql = FromDataframe(
                        target_table=target_table, target_schema=target_schema, dataframe=self.source_dataframe
                    ).upsert(
                        source_table=temp_target_name,
                        match_condition=match_list,
                        dbms_output=dbms_dialect,
                        constraint_columns=constraint_list,
                    )
                    upsert_statement = text(merge_sql)
                    if dbms_dialect == 'sqlite':
                        params_list = self.source_dataframe.to_dict(orient='records')
                        session.execute(upsert_statement, params_list)

                    else:
                        session.execute(upsert_statement)

                    session.execute(drop_temp_table)
                    session.commit()
                except SQLAlchemyError as e:
                    session.rollback()
                    print(f"An error occurred during batch insertion: {e}")
                    raise
                finally:
                    session.close()

        ### Execution Order ###
        # 1 Create temp table
        create_temp_table()
        # Load Into Temp
        batch_insert()
        # Execute Merge
        execute_merge()
