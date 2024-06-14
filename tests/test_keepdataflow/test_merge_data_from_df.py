import unittest

import pandas as pd

from keepdataflow.sql_conn import SqlConn


class TestMergeDataFromDataFrame(unittest.TestCase):
    def setUp(self) -> None:
        self.sql_db2 = "sqlite:////Users/themobilescientist/Documents/projects/archive/keepitsql/test.db"
        self.data = {
            "ItemID": ["ID101", "ID102", "ID103", "ID104"],
            "ItemName": ["Laptop", "Desk Chair", "USB-C Cable", "Monitor"],
            "Description": [
                "15-inch laptop with 8GB RAM",
                "Ergonomic office chair",
                "1m USB-C charging cable",
                "24-inch LED monitors",
            ],
            "Category": ["Electronics", "Furniture", "Electronics", "Electronics"],
            "Quantity": [10, 5, 50, 8],
            "Location": ["Warehouse A", "Warehouse F", "Warehouse A", "Warehouse C"],
        }
        self.df = pd.DataFrame(self.data)
        self.database_connection = SqlConn(database_url=self.sql_db2)

    def test_merge_data(self) -> None:
        self.database_connection.merge_data(
            source_dataframe=self.df,
            target_table="human",
            match_condition=['ItemID'],
            dbms_type='sqlite',
        )
        # Here, you would add assertions to check the effects of the merge
        # For example, you can check if the data was correctly merged into the target table
        # This can be done by querying the database and verifying the contents of the target table


# if __name__ == '__main__':
#     unittest.main()

data = {
    "ItemID": ["ID101", "ID102", "ID103", "ID104"],
    "ItemName": ["Laptop", "Desk Chair", "USB-C Cable", "Monitor"],
    "Description": [
        "15-inch laptop with 8GB RAM",
        "Ergonomic office chair",
        "1m USB-C charging cable",
        "24-inch LED monitors",
    ],
    "Category": ["Electronics", "Furniture", "Electronics", "Electronics"],
    "Quantity": [10, 5, 50, 8],
    "Location": ["Warehouse A", "Warehouse F", "Warehouse A", "Warehouse C"],
}
df = pd.DataFrame(data)
sql_db2 = "sqlite:////Users/themobilescientist/Documents/projects/archive/keepitsql/test.db"
SqlConn(sql_db2).DataframeToDatabase