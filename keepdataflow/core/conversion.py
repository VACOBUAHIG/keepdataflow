from typing import (
    Optional,
    Union,
)

import pandas as pd
import sqlalchemy
from sqlalchemy import (
    create_engine,
    inspect,
)
from sqlalchemy.orm import (
    Session,
    sessionmaker,
)

from .conversion_rules import data_types

# from sqlalchemy.types import BOOLEAN, INTEGER, BIGINT, FLOAT, NUMERIC, DATE, TIMESTAMP, TEXT, VARCHAR, BINARY, JSON, JSONB, UUID


# Function to map SQL dtype to pandas dtype
def map_sql_to_pandas_dtype(sql_type: str) -> Optional[str]:
    for dtype in data_types:
        if dtype["sql_type"] == sql_type:
            return dtype["pandas_type"]
    return None


# Function to read SQL table schema and convert DataFrame columns to corresponding pandas dtypes
def read_sql_table_and_convert_types(
    df: pd.DataFrame, db_resource: Union[str, Session], table_name: str, schema_name: Optional[str] = None
) -> pd.DataFrame:
    if isinstance(db_resource, str):
        engine = create_engine(db_resource)
        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()
        session_provided = False
    else:
        session = db_resource
        session_provided = True

    if session.bind is None:
        raise ValueError("Session is not bound to an engine or connection")

    inspector = inspect(session.bind)
    columns = inspector.get_columns(table_name, schema=schema_name)
    for column in columns:
        col_name = column["name"]
        sql_type = column["type"].__class__.__name__.lower()
        pandas_type = map_sql_to_pandas_dtype(sql_type)
        if pandas_type is not None and col_name in df.columns:
            if pandas_type in ["datetime64[ns]", "datetime"]:
                df[col_name] = pd.to_datetime(df[col_name])
            elif pandas_type == "date":
                df[col_name] = pd.to_datetime(df[col_name]).dt.date
            else:
                df[col_name] = df[col_name].astype(pandas_type)
        print(f"Column: {col_name}, SQL Type: {sql_type}, Pandas Type: {pandas_type}")

    if not session_provided:
        session.close()

    return df
