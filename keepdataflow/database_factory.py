from abc import ABC, abstractmethod
import sqlalchemy as sa


class DatabaseFactory(ABC):
    @abstractmethod
    def create_source_connection(self):
        pass

    @abstractmethod
    def create_destination_connection(self):
        pass


class SQLServerFactory(DatabaseFactory):
    def __init__(self, source_conn_str, destination_conn_str):
        self.source_conn_str = source_conn_str
        self.destination_conn_str = destination_conn_str

    def create_source_connection(self):
        return sa.create_engine(self.source_conn_str)

    def create_destination_connection(self):
        return sa.create_engine(self.destination_conn_str)


class PostgresFactory(DatabaseFactory):
    def __init__(self, source_conn_str, destination_conn_str):
        self.source_conn_str = source_conn_str
        self.destination_conn_str = destination_conn_str

    def create_source_connection(self):
        return sa.create_engine(self.source_conn_str)

    def create_destination_connection(self):
        return sa.create_engine(self.destination_conn_str)
