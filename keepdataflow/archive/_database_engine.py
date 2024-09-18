from threading import Lock
from typing import (
    Optional,
    Type,
)

from sqlalchemy import (
    Engine,
    create_engine,
)
from sqlalchemy.orm import (
    Session,
    sessionmaker,
)


class DatabaseEngine:
    """
    A database engine class that manages SQLAlchemy engine and session creation.

    Attributes:
        connection_string (str): The database connection string.
        engine (Optional[Engine]): The SQLAlchemy engine instance.
        SessionLocal (sessionmaker): The sessionmaker factory for creating sessions.
    """

    _engine: Optional[Engine] = None
    _lock: Lock = Lock()

    def __init__(self, connection_string: str) -> None:
        """
        Initialize the DatabaseEngine with a connection string.

        Args:
            connection_string (str): The database connection string.
        """
        self.connection_string: str = connection_string
        self.engine: Engine = self._get_engine()
        self.SessionLocal: sessionmaker = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def _get_engine(self) -> Engine:
        """
        Get the SQLAlchemy engine, creating it if it doesn't exist.

        Returns:
            Engine: The SQLAlchemy engine instance.
        """
        if DatabaseEngine._engine is None:
            with DatabaseEngine._lock:
                if DatabaseEngine._engine is None:
                    DatabaseEngine._engine = create_engine(self.connection_string)
        return DatabaseEngine._engine

    def create_session(self) -> Session:
        """
        Create a new SQLAlchemy session.

        Returns:
            Session: The SQLAlchemy session.
        """
        return self.SessionLocal()

    def get_dbms_dialect(self):
        return self.SessionLocal().bind.dialect.name

    def __enter__(self) -> Session:
        """
        Enter the runtime context related to this object.

        Returns:
            Session: The SQLAlchemy session.
        """
        self.session: Session = self.create_session()
        return self.session

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Type[BaseException]],
    ) -> None:
        """
        Exit the runtime context related to this object.

        Args:
            exc_type (Optional[Type[BaseException]]): The exception type.
            exc_val (Optional[BaseException]]): The exception value.
            exc_tb (Optional[Type[BaseException]]): The traceback object.
        """
        try:
            if exc_type is not None:
                self.session.rollback()
            else:
                self.session.commit()
        finally:
            self.session.close()
