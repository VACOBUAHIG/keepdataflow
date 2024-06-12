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
)
from pandas import DataFrame
from sqlalchemy import (
    create_engine,
    text,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Session as SessionType
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


def generate_database_connection(database_url: str, py_database_module: Any) -> Any:
    db_url = database_url
    connection = py_database_module.connect(db_url)
    return connection


class DataframeToDatabase:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.db_engine = create_engine(self.database_url)
        self.Session = sessionmaker(bind=self.db_engine)

    def refresh_data(
        self,
        source_dataframe: Any,
        target_table: str,
        database_module: Any,
        target_schema: Optional[str] = None,
        batch_size: int = 1000,
        select_column: Optional[List[str]] = None,
        temp_type: Optional[str] = None,
    ) -> None:
        """Refreshes data in a target table by truncating and reloading from a source DataFrame.

        Args:
            source_dataframe (DataFrame): The source data.
            target_table (str): The target table name.
            database_module (Any): The database module to use, e.g., psycopg2, sqlite3, etc.
            target_schema (Optional[str]): The target schema, if applicable. Default is None.
            batch_size (int): The batch size for inserting data. Default is 1000.
            select_column (Optional[List[str]]): List of columns to select. Default is None.
            temp_type (Optional[str]): Temporary table type, if applicable. Default is None.
        Return:
            None
        """
        truncate_table = f"DELETE FROM {target_table}"

        connection = generate_database_connection(database_url=self.database_url, py_database_module=database_module)
        cursor = connection.cursor()
        load_df = FromDataframe(target_table="human", target_schema=None)

        try:
            cursor.execute(truncate_table)

            for start in range(0, len(source_dataframe), batch_size):
                batch_data = source_dataframe[start : start + batch_size]
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
        source_dataframe: Any,
        target_table: str,
        dbms_type: str,
        match_condition: List[str],
        constraint_columns: Optional[List[str]] = None,
        target_schema: Optional[str] = None,
        batch_size: int = 1000,
    ) -> None:
        """Merges data from a source DataFrame into a target table.

        Args:
            source_dataframe (DataFrame): The source data.
            target_table (str): The target table name.
            dbms_type (str): The type of the database management system (e.g., 'mssql').
            match_condition (List[str]): List of conditions to match records.
            constraint_columns (Optional[List[str]]): List of constraint columns. Default is None.
            target_schema (Optional[str]): The target schema, if applicable. Default is None.
            batch_size (int): The batch size for inserting data. Default is 1000.

        Return:
            None
        """
        uid = "".join(random.choices(string.ascii_lowercase, k=4))
        temp_name = f"_source_{target_table}_{uid}"

        if dbms_type == 'mssql':
            temp_target_name = f'##{temp_name}'
        elif dbms_type == 'mssql_local':
            temp_target_name = f'#{temp_name}'
        else:
            temp_target_name = temp_name

        drop_temp_table = text(f"DROP TABLE {temp_target_name}")

        source_table, temp_table = CopyDDl(
            database_url=self.database_url, local_table_name=target_table, local_schema_name=target_schema
        ).create_ddl(new_table_name=temp_name, temp_dll_output=dbms_type, drop_primary_key='Y')

        with self.Session() as session:
            try:
                inspector = inspect(self.db_engine)
                primary_key_list = inspector.get_pk_constraint(target_table, schema=target_schema)

                if constraint_columns is None:
                    constraint_columns = primary_key_list['constrained_columns']
                else:
                    constraint_columns.extend(primary_key_list['constrained_columns'])

                create_temp_table = text(temp_table)
                session.execute(create_temp_table)
                print("Temp table create complete")

                for start in range(0, len(source_dataframe), batch_size):
                    batch_data = source_dataframe[start : start + batch_size]
                    insert_temp = FromDataframe(
                        target_table=temp_target_name, target_schema=None, dataframe=batch_data
                    ).insert()

                    insert_sql = text(insert_temp)
                    session.execute(insert_sql)
                print("Temp table load complete")

                merge_sql = FromDataframe(
                    target_table=target_table, target_schema=target_schema, dataframe=source_dataframe
                ).upsert(
                    source_table=temp_target_name,
                    match_condition=match_condition,
                    dbms_output=dbms_type,
                    constraint_columns=constraint_columns,
                )
                upsert_statement = text(merge_sql)

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
