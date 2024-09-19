import polars as pl


def df_insert_on_conflict(data_frame, table_name, engine, schema=None, conflict_columns=None):
    """
    Perform an insert on conflict (upsert) on a PostgreSQL table from a Polars DataFrame.
    Constructs an INSERT INTO ... ON CONFLICT statement.
    """
    table_spec = ""
    if schema:
        table_spec += schema + "."

    table_spec += table_name

    df_columns = list(data_frame.columns)

    # Build the INSERT ON CONFLICT statement
    conflict_target = ", ".join([f"{col}" for col in conflict_columns])
    update_list = ", ".join([f"{col} = EXCLUDED.{col}" for col in df_columns if col not in conflict_columns])

    stmt = f'''
        INSERT INTO {table_spec} ({', '.join([col for col in df_columns])})
        VALUES ({', '.join(['%s' for _ in df_columns])})
        ON CONFLICT ({conflict_target})
        DO UPDATE SET {update_list}
    '''

    # Convert Polars DataFrame to list of tuples for fast insertion
    data = [tuple(row) for row in data_frame.rows()]

    with engine.begin() as conn:
        cursor = conn.connection.cursor()
        cursor.executemany(stmt, data)
        conn.connection.commit()
