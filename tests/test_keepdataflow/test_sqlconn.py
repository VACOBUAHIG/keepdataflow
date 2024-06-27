import unittest

from sqlalchemy import (
    Engine,
    create_engine,
)
from sqlalchemy.orm import (
    Session,
    sessionmaker,
)

from keepdataflow.core.database_to_database import DatabaseToDatabase
from keepdataflow.core.dataframe_to_db import DataframeToDatabase
from keepdataflow.sql_conn import SqlConn


class TestSqlConn(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Setup the database and SqlConn instance before running any tests."""
        cls.database_url = 'sqlite:////Users/themobilescientist/Documents/projects/archive/keepitsql/test.db'
        cls.sql_conn_instance = SqlConn(cls.database_url)

    def test_engine_creation(self):
        """Test if the engine is created correctly and is a singleton."""
        engine1 = self.sql_conn_instance._get_engine()
        engine2 = self.sql_conn_instance._get_engine()
        self.assertIs(engine1, engine2)
        self.assertIsInstance(engine1, Engine)

    def test_session_creation(self):
        """Test if a session can be created successfully."""
        session = self.sql_conn_instance.get_session()
        self.assertIsInstance(session, Session)
        session.close()

    def test_dataframe_to_db_initialization(self):
        """Test if DataframeToDatabase is initialized correctly."""
        self.assertIsInstance(self.sql_conn_instance.df_to_db, DataframeToDatabase)

    def test_database_to_database_initialization(self):
        """Test if DatabaseToDatabase is initialized correctly."""
        self.assertIsInstance(self.sql_conn_instance.db_to_db, DatabaseToDatabase)

    def test_delegate_attribute_access(self):
        """Test if attribute access is delegated correctly."""
        with self.assertRaises(AttributeError):
            self.sql_conn_instance.non_existent_method()

    def test_wrapper_functionality(self):
        """Test if the wrapper raises an exception when source_dataframe is not set."""
        df_to_db = self.sql_conn_instance.df_to_db
        if hasattr(df_to_db, 'source_dataframe'):
            del df_to_db.source_dataframe

        def sample_method():
            pass

        setattr(df_to_db, 'sample_method', sample_method)

        with self.assertRaises(AttributeError):
            self.sql_conn_instance.sample_method()


# Run the tests
if __name__ == '__main__':
    unittest.main()
