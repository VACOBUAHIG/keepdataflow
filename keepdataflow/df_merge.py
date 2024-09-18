import uuid
import polars as pl
import sqlalchemy as sa

destinationserverAddress = "VHACDWA01.VHA.MED.VA.GOV"
destinationDatabaseName = "HEFP_EHRMSPCAM"


# Establish a connection using pyodbc


connectionString = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={destinationserverAddress};DATABASE={destinationDatabaseName};Trusted_Connection=yes;"


# class DataframeSQLOperations:
#     def __init__(self, dataframe, target_table_name,target_engine,target_schema):
def df_merge(
    data_frame,
    table_name,
    engine=None,
    schema=None,
    match_columns: list = None,
    insert_match_column=True,
    chunksize=None,
    dtype=None,
    skip_inserts=False,
    skip_updates=False,
):
    """
    Perform an "upsert" on a SQL Server table from a Polars DataFrame.
    Constructs a T-SQL MERGE statement, uploads the DataFrame to a
    temporary table, and then executes the MERGE.

    Parameters
    ----------
    data_frame : polars.DataFrame
        The DataFrame to be upserted.
    table_name : str
        The name of the target table.
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy Engine to use.
    schema : str, optional
        The name of the schema containing the target table.
    match_columns : list of str, optional
        A list of the column name(s) on which to match. If omitted, the
        primary key columns of the target table will be used.
    chunksize: int, optional
        Specify chunk size for .to_sql(). See the pandas docs for details.
    dtype : dict, optional
        Specify column types for .to_sql(). See the pandas docs for details.
    skip_inserts : bool, optional
        Skip inserting unmatched rows. (Default: False)
    skip_updates : bool, optional
        Skip updating matched rows. (Default: False)
    """
    if skip_inserts and skip_updates:
        raise ValueError("skip_inserts and skip_updates cannot both be True")

    temp_table_name = "##" + str(uuid.uuid4()).replace("-", "_")

    table_spec = ""
    if schema:
        table_spec += "[" + schema.replace("]", "]]") + "]."

    table_spec += "[" + table_name.replace("]", "]]") + "]"

    df_columns = list(data_frame.columns)

    # Use SQLAlchemy inspect to get match columns and identity (auto-increment) columns
    insp = sa.inspect(engine)
    table_info = insp.get_columns(table_name, schema=schema)

    # # If match_columns are not provided, get the primary key columns
    if not match_columns:
        match_columns = insp.get_pk_constraint(table_name, schema=schema)["constrained_columns"]

        # Identify identity (auto-increment) columns
    # identity_columns = [col["name"] for col in table_info if col.get("autoincrement") == True]

    # Identify identity (auto-increment) columns
    identity_columns = [col["name"] for col in table_info if col.get("autoincrement") == True]

    # Columns to update in the match clause
    columns_to_update = [col for col in df_columns if col not in match_columns]

    stmt = f"MERGE {table_spec} WITH (HOLDLOCK) AS main\n"
    stmt += f"USING (SELECT {', '.join([f'[{col}]' for col in df_columns])} FROM {temp_table_name}) AS temp\n"
    join_condition = " AND ".join([f"main.[{col}] = temp.[{col}]" for col in match_columns])
    stmt += f"ON ({join_condition})"

    if not skip_updates:
        stmt += "\nWHEN MATCHED THEN\n"
        update_list = ", ".join([f"[{col}] = temp.[{col}]" for col in columns_to_update])
        stmt += f"  UPDATE SET {update_list}"

    if not skip_inserts:
        stmt += "\nWHEN NOT MATCHED THEN\n"

        # Exclude identity columns and optionally match columns from the insert statement
        insert_columns = [col for col in df_columns if col not in identity_columns]

        if not insert_match_column:
            insert_columns = [col for col in insert_columns if col not in match_columns]

        insert_cols_str = ",".join([f"[{col}]\n" for col in insert_columns])
        insert_vals_str = ",".join([f"temp.[{col}]\n" for col in insert_columns])
        stmt += f"  INSERT ({insert_cols_str}) VALUES ({insert_vals_str})"
    stmt += ";"

    with engine.begin() as conn:
        # Insert data into the temp table using Arrow
        # pl.from_records(arrow_table.to_pandas()).to_sql(temp_table_name, conn, index=False, chunksize=chunksize)
        data_frame.write_database(table_name=temp_table_name, connection=conn)
        conn.exec_driver_sql(stmt)
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {temp_table_name}")


# engine = sa.create_engine(
#     f"mssql+pyodbc://{destinationserverAddress}/{destinationDatabaseName}" "?driver=ODBC+Driver+17+for+SQL+Server",
#     fast_executemany=True,
# )


# with engine.begin() as conn:
#     conn.exec_driver_sql("DROP TABLE IF EXISTS CAP_VHA.main_table")
#     conn.exec_driver_sql(
#         "CREATE TABLE CAP_VHA.main_table (id int primary key Identity(1,1), txt nvarchar(50), status nvarchar(50))"
#     )
#     # conn.exec_driver_sql("INSERT INTO main_table (id, txt, status) VALUES (1, N'row 1 old text', N'original')")

#     df = pl.DataFrame({"id": [2, 1], "txt": ["new row 2 text", "row 1 new text"], "status": ["upserted", "upserted"]})
#     print(df)
# df_merge(data_frame=df, table_name="main_table", engine=engine, schema='CAP_VHA', match_columns=['id'])


# class DatabasetoDatabaseCopy

# # if __name__ == "__main__":
# #     # Example usage
# #     engine = sa.create_engine(
# #         "mssql+pyodbc://scott:tiger^5HHH@192.168.0.199/test" "?driver=ODBC+Driver+17+for+SQL+Server",
# #         fast_executemany=True,
# #     )

# #     # Create example environment
# #     with engine.begin() as conn:
# #         conn.exec_driver_sql("DROP TABLE IF EXISTS main_table")
# #         conn.exec_driver_sql("CREATE TABLE main_table (id int primary key, txt nvarchar(50), status nvarchar(50))")
# #         conn.exec_driver_sql("INSERT INTO main_table (id, txt, status) VALUES (1, N'row 1 old text', N'original')")

# #     # Create a Polars DataFrame to upsert
# #     df = pl.DataFrame({"id": [2, 1], "txt": ["new row 2 text", "row 1 new text"], "status": ["upserted", "upserted"]})

# #     df_upsert(df, "main_table", engine)

# #     # Check results
# #     with engine.begin() as conn:
# #         print(conn.exec_driver_sql("SELECT * FROM main_table").all())
# #         # Should output: [(1, 'row 1 new text', 'upserted'), (2, 'new row 2 text', 'upserted')]
