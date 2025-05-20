from sqlalchemy.engine import Engine
from typing import Dict, Any, Optional, Literal, List
from sqlalchemy import text
import os


def run_stored_procedure(
    engine: Engine,
    procedure_name: str,
    params: Optional[Dict[str, Any]] = None,
    on_fail: Literal["continue", "fail"] = "fail",
) -> List[Any]:
    """
    Executes a stored procedure using SQLAlchemy, adapting to the DBMS.

    Args:
        engine: SQLAlchemy engine or connection.
        procedure_name (str): Name of the stored procedure.
        params (dict or None): Parameters for the stored procedure.
        on_fail (str): "continue" to continue on failure, "fail" to raise exception.

    Returns:
        List of results for the procedure executed.
    """
    if params is None:
        params = {}

    # Detect DBMS
    dbms = engine.dialect.name.lower()

    # Build SQL based on DBMS
    if dbms in ("mssql", "sqlserver"):
        # SQL Server: EXEC proc_name @param1=:param1, @param2=:param2
        if params:
            param_str = ', '.join([f"@{k}=:{k}" for k in params.keys()])
            sql = f"EXEC {procedure_name} {param_str}"
        else:
            sql = f"EXEC {procedure_name}"
    elif dbms == "mysql":
        # MySQL: CALL proc_name(:param1, :param2)
        placeholders = ', '.join([f":{k}" for k in params.keys()]) if params else ''
        sql = f"CALL {procedure_name}({placeholders})"
    elif dbms == "postgresql":
        # PostgreSQL: CALL proc_name(:param1, :param2) or SELECT * FROM proc_name(:param1, :param2)
        placeholders = ', '.join([f":{k}" for k in params.keys()]) if params else ''
        sql = f"CALL {procedure_name}({placeholders})" if placeholders else f"CALL {procedure_name}()"
    else:
        # Default: try CALL
        placeholders = ', '.join([f":{k}" for k in params.keys()]) if params else ''
        sql = f"CALL {procedure_name}({placeholders})" if placeholders else f"CALL {procedure_name}()"

    results = []
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            conn.connection.commit()
            try:
                results.append(result.fetchall())
            except Exception:
                results.append(None)
    except Exception:
        if on_fail == "fail":
            raise
        else:
            results.append(None)

    return results


def run_sql_query(
    engine: Engine,
    sql_file: Optional[str] = None,
    sql_text: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    on_fail: Literal["continue", "fail"] = "fail",
) -> List[Any]:
    """
    Executes a SQL query or command using SQLAlchemy, from a .sql file or a string.

    Args:
        engine: SQLAlchemy engine or connection.
        sql_file (str, optional): Path to a .sql file containing the query.
        sql_text (str, optional): SQL query as a string.
        params (dict or None): Parameters for the SQL query.
        on_fail (str): "continue" to continue on failure, "fail" to raise exception.

    Returns:
        List of results for the query executed.
    """
    if (sql_file and sql_text) or (not sql_file and not sql_text):
        raise ValueError("Provide either sql_file or sql_text, but not both or neither.")

    if sql_file:
        if not sql_file.lower().endswith('.sql'):
            raise ValueError("sql_file must have a .sql extension.")
        if not os.path.isfile(sql_file):
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
        with open(sql_file, "r", encoding="utf-8") as f:
            sql = f.read()
    else:
        sql = sql_text

    if params is None:
        params = {}

    results = []
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            conn.connection.commit()
            try:
                results.append(result.fetchall())
            except Exception:
                results.append(None)
    except Exception:
        if on_fail == "fail":
            raise
        else:
            results.append(None)

    return results
