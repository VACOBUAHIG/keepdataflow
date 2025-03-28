import polars as pl
from keepdataflow.database_operations.df_merge import df_merge
from keepdataflow.database_operations.df_insert_on_conflict import df_insert_on_conflict
from keepdataflow.database_operations.df_insert import df_insert
from abc import ABC, abstractmethod
from sqlalchemy.engine.url import make_url

# from database_factory import DatabaseFactory

# from database_factory


class DataTransfer(ABC):
    @abstractmethod
    def transfer(self, source_table, destination_table, source_schema, destination_schema, operation="merge", **kwargs):
        pass


class DatabaseDataTransfer(DataTransfer):
    def __init__(self, source_engine, destination_engine):
        """
        Initialize the DatabaseDataTransfer instance.

        Parameters:
        ----------
        source_engine : SQLAlchemy Engine
            The engine used to connect to the source database.
        destination_engine : SQLAlchemy Engine
            The engine used to connect to the destination database.
        operation : str
            The type of operation to perform ("merge" for upsert, "insert" for basic insert).
        """
        self.source_engine = source_engine
        self.destination_engine = destination_engine
        # self.operation = operation  # "merge" or "insert"

        # Automatically determine the db_type using SQLAlchemy dialect

        self.db_type = self.destination_engine.dialect.name  # e.g., "postgresql" or "mssql"

    def transfer(self, source_table, destination_table, source_schema, destination_schema, operation="merge", **kwargs):
        """Perform data transfer between the source and destination tables using either
        MERGE (for SQL Server), INSERT ON CONFLICT (for PostgreSQL), or a basic INSERT.

        This function dynamically handles SQL Server and PostgreSQL operations and passes
        relevant arguments to the corresponding transfer methods (`df_merge`, `df_insert_on_conflict`, or `df_insert`).

        Parameters
        ----------
        source_table : str
            The name of the table in the source database from which data will be read.
        destination_table : str
            The name of the table in the destination database to which data will be transferred.
        source_schema : str
            The schema of the source table.
        destination_schema : str
            The schema of the destination table.
        **kwargs : dict
            Additional arguments required for specific operations. These depend on the operation
            being performed (merge/upsert or insert) and the database type (SQL Server or PostgreSQL).

            Available kwargs:

            - For `df_merge` (SQL Server MERGE):
                - match_columns (list of str): The columns to use for matching records (typically primary keys).
                - include_match_condition_in_insert (bool): Whether to include match columns in the INSERT statement. Default is True.
                - skip_inserts (bool): Whether to skip inserting unmatched rows. Default is False.
                - skip_updates (bool): Whether to skip updating matched rows. Default is False.

            - For `df_insert_on_conflict` (PostgreSQL INSERT ON CONFLICT):
                - conflict_columns (list of str): The columns to use in the ON CONFLICT clause for resolving conflicts (typically primary keys).

            - For `df_insert` (Basic INSERT for both databases):
                - No additional kwargs required for basic INSERT.

        Raises:
        -------
        ValueError
            If an unsupported operation or database type is provided.

        Example Usage:
        --------------
        # SQL Server MERGE example
        data_transfer.transfer(
            source_table="source_table_1",
            destination_table="destination_table_1",
            source_schema="dbo",
            destination_schema="dbo",
            match_columns=["id"],  # SQL Server specific
            include_match_condition_in_insert=True,
            skip_inserts=False,
            skip_updates=True
        )

        # PostgreSQL INSERT ON CONFLICT example
        data_transfer.transfer(
            source_table="source_table_1",
            destination_table="destination_table_1",
            source_schema="public",
            destination_schema="public",
            conflict_columns=["id"]  # PostgreSQL specific
        )

        # Basic INSERT example (SQL Server or PostgreSQL)
        data_transfer.transfer(
            source_table="source_table_1",
            destination_table="destination_table_1",
            source_schema="dbo",
            destination_schema="dbo"
        )
        """
        # Build the source table specification with schema
        if source_schema:
            source_table_spec = f"{source_schema}.{source_table}"
        else:
            source_table_spec = f"{source_table}"

        # Read data from the source table
        with self.source_engine.connect() as conn:
            query = f'SELECT * FROM {source_table_spec}'
            data_frame = pl.read_database(query, conn)
            print(data_frame)

        # # # Handle operations based on db_type and operation
        if operation == 'merge':
            if self.db_type == 'mssql':  # SQL Server (Microsoft SQL Server uses "mssql")
                # Call df_merge with its specific parameters
                df_merge(
                    data_frame,
                    destination_table,
                    self.destination_engine,
                    schema=kwargs.get("destination_schema", destination_schema),
                    match_columns=kwargs.get("match_columns"),
                    insert_match_column=kwargs.get("insertMatchColumn", True),
                    skip_inserts=kwargs.get("skip_inserts", False),
                    skip_updates=kwargs.get("skipUpdates", False),
                    skip_deletes=kwargs.get("skipDeletes", False),
                )
            elif self.db_type == "postgresql":  # PostgreSQL
                # Call df_insert_on_conflict with its specific parameters
                df_insert_on_conflict(
                    data_frame,
                    destination_table,
                    self.destination_engine,
                    schema=kwargs.get('destination_schema', destination_schema),
                    conflict_columns=kwargs.get('conflict_columns'),
                )
        elif operation == 'append':
            # Call df_insert with its specific parameters
            df_insert(
                data_frame,
                destination_table,
                self.destination_engine,
                schema=kwargs.get('destination_schema', destination_schema),
                truncate_table='N',
            )
        elif operation == 'refresh':
            # Call df_insert with its specific parameters
            df_insert(
                data_frame,
                destination_table,
                self.destination_engine,
                schema=kwargs.get('destination_schema', destination_schema),
                truncate_table='Y',
            )
        else:
            raise ValueError(f'Unsupported operation: {operation}')
