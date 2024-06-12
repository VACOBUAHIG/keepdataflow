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


if __name__ == '__main__':
    unittest.main()


# class TestSqlConn(unittest.TestCase):

#     def setUp(self):
#         self.sql_db2 = "sqlite:///:memory:"
#         self.data = {
#             "ItemID": ["ID101", "ID102", "ID103", "ID104"],
#             "ItemName": ["Laptop", "Desk Chair", "USB-C Cable", "Monitor"],
#             "Description": [
#                 "15-inch laptop with 8GB RAM",
#                 "Ergonomic office chair",
#                 "1m USB-C charging cable",
#                 "24-inch LED monitors",
#             ],
#             "Category": ["Electronics", "Furniture", "Electronics", "Electronics"],
#             "Quantity": [10, 5, 50, 8],
#             "Location": ["Warehouse A", "Warehouse F", "Warehouse A", "Warehouse C"],
#         }
#         self.df = pd.DataFrame(self.data)


#         # Create sample table based on the dataframe
#         self.metadata = MetaData()
#         self.sample_table = Table(
#             'human', self.metadata,
#             Column('ItemID', String, primary_key=True),
#             Column('ItemName', String),
#             Column('Description', String),
#             Column('Category', String),
#             Column('Quantity', Integer),
#             Column('Location', String)
#         )
#         self.metadata.create_all(self.sql_conn.db_engine)

#         self.sql_conn = SqlConn(self.sql_db2)

#         # Insert initial data into the sample table
#         with self.sql_conn.db_engine.connect() as conn:
#             self.df.to_sql('human', conn, if_exists='append', index=False)

#     def test_merge_data(self):
#         merge_sql = self.sql_conn.merge_data(
#             source_dataframe=self.df,
#             target_table="human",
#             match_condition=['ItemID'],
#             dbms_type='sqlite'
#         )
#         self.assertIn("Generated SQL merge statement", merge_sql)
#         print(merge_sql)

# if __name__ == '__main__':
#     unittest.main()
