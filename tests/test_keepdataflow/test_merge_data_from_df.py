import unittest

import numpy as np
import pandas as pd
import polars as pl

from keepdataflow.sql_conn import SqlConn


class TestMergeDataFromDataFrame(unittest.TestCase):
    def setUp(self) -> None:
        self.sql_db2 = "sqlite:////Users/themobilescientist/Documents/projects/archive/keepitsql/test.db"
        self.example_data = {
            "ItemID": ["1088100", "1029900", "103888", "104999"],
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

        # Create a DataFrame from the example data
        self.df = pd.DataFrame(self.example_data)
        self.pl_df = pl.DataFrame(self.example_data)

        # Generate a DataFrame with 50,000 rows
        self.df_50k = self.generate_random_data(50000)
        self.pl_df_50k = pl.DataFrame(self.df_50k)

        # Initialize database connection
        self.database_connection = SqlConn(database_url=self.sql_db2)

    def generate_random_data(self, n):
        np.random.seed(42)  # For reproducibility
        warehouse_names = ["Warehouse A", "Warehouse B", "Warehouse C", "Warehouse D", "Warehouse K"]
        data = {
            "ItemID": np.random.permutation(np.arange(1, n + 1)),
            "ItemName": np.random.choice(self.example_data["ItemName"], n),
            "Description": np.random.choice(self.example_data["Description"], n),
            "Category": np.random.choice(self.example_data["Category"], n),
            "Quantity": np.random.randint(1, 100, n),
            "Location": np.random.choice(warehouse_names, n),  # Randomly select warehouse names
        }
        return pd.DataFrame(data)

    # def test_crate_temp_table(self) -> None:
    #     self.database_connection.operations.create_temp_table(table_name="human")

    # def test_load_dataframe(self):
    #     self.database_connection.load_dataframe(self.pl_df)

    # def test_load_dataframe_insert(self):
    #     self.database_connection.from_dataframe(self.pl_df).db_insert("human",full_refresh='Y')

    def test_load_dataframe_insert_on_conflict(self):
        # print(self.pl_df_50k)
        self.database_connection.from_dataframe(self.pl_df_50k).db_merge("human")

    # def test_load_df(self) -> None:
    #     self.database_connection.df_to_db.load_df(self.pl_df)
    #     # self.database_connection.merge_data(
    #     #     target_table="human",
    #     #     match_condition=['ItemID'],
    #     # )
    # def test_merge_data(self) -> None:
    #     self.database_connection.df_to_db.load_df(self.df)
    #     self.database_connection.merge_data(
    #         target_table="human",
    #         match_condition=['ItemID'],
    #     )
    # Here, you would add assertions to check the effects of the merge
    # For example, you can check if the data was correctly merged into the target table
    # This can be done by querying the database and verifying the contents of the target table

    # def test_merge_data_with_out_match(self) -> None:
    #     self.database_connection.df_to_db.load_df(self.df)
    #     self.database_connection.merge_data(
    #         target_table="human",
    #         # match_condition=['ItemID'],
    #     )
    # Here, you would add assertions to check the effects of the merge
    # For example, you can check if the data was correctly merged into the target table
    # This can be done by querying the database and verifying the contents of the target table

    # def test_copy_and_merge_data(self) -> None:
    #     sql_db3 = "sqlite:////Users/themobilescientist/Documents/projects/archive/keepitsql/source_test.db"
    #     self.database_connection.db_to_db.copy_source_db(sql_db3, 'human')
    #     self.database_connection.merge_data(
    #         target_table="human",
    #         match_condition=['ItemID'],
    #     )

    # def test_databse_to_databsase_copy_with_query(self) -> None:
    #     sql_db3 = "sqlite:////Users/themobilescientist/Documents/projects/archive/keepitsql/source_test.db"
    #     self.database_connection.db_to_db.copy_source_db(
    #         source_db_url=sql_db3, source_query="Select * From human", source_table_name='human'
    #     )
    #     self.database_connection.merge_data(
    #         target_table="human",
    #         match_condition=['ItemID'],
    #     )
    # # Add assertions to check the effects of the merge
    # print(self.database_connection.get_session)


if __name__ == '__main__':
    unittest.main()
