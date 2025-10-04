import sqlite3
import threading
from contextlib import contextmanager
from typing import Dict, List, Optional, Generator

from .config import db_config,MAX_QUERY_TIMEOUT
from .logging import get_logger

logger = get_logger(__name__)

class DatabaseManager:
    """Manages SQLite database connections and operations."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = None
        self.lock = threading.Lock()
        self.connect()

    def connect(self):
        """Establish a database connection."""
        try:
            self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self.connection.row_factory = sqlite3.Row
            logger.info(f"Connected to database at {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            raise

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed.")

    @contextmanager
    def get_cursor(self) -> Generator[sqlite3.Cursor, None, None]:
        """Context manager for database cursor."""
        with self.lock:
            cursor = self.connection.cursor()
            try:
                yield cursor
                self.connection.commit()
            except sqlite3.Error as e:
                self.connection.rollback()
                logger.error(f"Database operation error: {e}")
                raise
            finally:
                cursor.close()

    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict]:
        """Execute a query and return results as a list of dictionaries."""
        with self.get_cursor() as cursor:
            if params is None:
                params = ()
            logger.debug(f"Executing query: {query} with params: {params}")
            cursor.execute(query, params)
            rows = cursor.fetchall()
            result = [dict(row) for row in rows]
            logger.debug(f"Query returned {len(result)} rows.")
            return result

    def execute_non_query(self, query: str, params: Optional[tuple] = None) -> int:
        """Execute a non-query (INSERT, UPDATE, DELETE) and return affected row count."""
        with self.get_cursor() as cursor:
            if params is None:
                params = ()
            logger.debug(f"Executing non-query: {query} with params: {params}")
            cursor.execute(query, params)
            affected_rows = cursor.rowcount
            logger.debug(f"Non-query affected {affected_rows} rows.")
            return affected_rows


db_manager = DatabaseManager(db_config.get_connection_string)

def get_db_manager() -> DatabaseManager:
    """Get the singleton database manager instance."""
    return db_manager