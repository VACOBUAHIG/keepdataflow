import polars as pl


def df_insert(data_frame, table_name, engine, schema=None):
    """
    Perform a simple insert into a SQL Server or PostgreSQL table from a Polars DataFrame.
    """
    table_spec = ""
    if schema:
        table_spec += schema + "."

    table_spec += table_name

    df_columns = list(data_frame.columns)

    stmt = f"""
        INSERT INTO {table_spec} ({', '.join([col for col in df_columns])})
        VALUES ({', '.join(['?' for _ in df_columns])})
    """
    print(stmt)
    # Convert Polars DataFrame to list of tuples for fast insertion
    data = [tuple(row) for row in data_frame.rows()]
    print(data)

    with engine.begin() as conn:
        cursor = conn.connection.cursor()
        cursor.executemany(stmt, data)
        conn.connection.commit()
