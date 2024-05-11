from sqlalchemy import (
    create_engine,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from keepdataflow.DataFrameToDatabase.from_df import from_df


class Sql:
    def sql(self):
        print('x')


class SqlConn:
    def __init__(self, database_url) -> None:
        self.database_url = database_url
        self.db_engine = create_engine(self.database_url)
        self.Session = sessionmaker(bind=self.db_engine)

        self.from_df = from_df(self.Session)


# class from_df(merge):
#     def __init__(self, session) -> None:
#         self.session = Session()

sql_db2 = "sqlite:////Users/themobilescientist/Documents/projects/archive/keepitsql/test.db"

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
import sqlite3

import pandas as pd

df = pd.DataFrame(data)

samp = SqlConn(sql_db2)

samp.from_df.merge(
    dataframe=df, target_table="human", match_condition=['ItemID'], database_module=sqlite3, database_url=sql_db2
)
