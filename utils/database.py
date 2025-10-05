import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

from .config import MAX_QUERY_TIMEOUT, db_config
from .logging import get_logger

logger = get_logger(__name__)


def dict_factory(cursor, row):
    """Convert SQLite row to a dictionary."""
    fields = [columns[0] for columns in cursor.description]
    return {key: value for key, value in zip(fields, row, strict=False)}


class DatabaseManager:
    """Manages SQLite database connections and operations."""

    def __init__(self, db_path: Optional[str] = None):
        self._local = threading.local()
        self._db_path = db_path or db_config.db_path
        logger.info(f"Database manager initialized for : {self._db_path}")

    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local connection"""
        if not hasattr(self._local, "connection") or self._local.connection is None:
            self._local.connection = sqlite3.connect(
                self._db_path,
                timeout=db_config.timeout,
                isolation_level=db_config.isolation_level,
                check_same_thread=False,
            )
            self._local.connection.row_factory = dict_factory
        return self._local.connection

    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for database connection."""
        conn = None
        try:
            conn = self._get_connection()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {e}")
            raise

    @contextmanager
    def get_cursor(self) -> Generator[sqlite3.Cursor, None, None]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
            finally:
                cursor.close()

    def execute_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results as a list of dictionaries."""
        try:
            with self.get_cursor() as cursor:
                if params:
                    param_list = []
                    modified_query = query
                    for key, value in params.items():
                        if isinstance(value, list):
                            placeholders = ",".join(["?"] * len(value))
                            modified_query = modified_query.replace(
                                f":{key}", f"({placeholders})"
                            )
                            param_list.extend(value)
                        else:
                            modified_query = modified_query.replace(f":{key}", "?")
                            param_list.append(value)
                    cursor.execute(modified_query, param_list)
                else:
                    cursor.execute(query)

                return cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"Database operation error: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise

    def execute_query_with_timeout(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = MAX_QUERY_TIMEOUT,
    ) -> List[Dict[str, Any]]:
        """Execute a query with a timeout and return results as a list of dictionaries."""
        return self.execute_query(query, params)

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get the schema of a table as a dictionary."""
        query = f"PRAGMA table_info({table_name})"

        try:
            with self.get_cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

                if not rows:
                    raise ValueError(f"Table '{table_name}' does not exist.")

                type_mapping = {
                    "INTEGER": "INTEGER",
                    "INT": "INTEGER",
                    "REAL": "REAL",
                    "FLOAT": "REAL",
                    "DOUBLE": "REAL",
                    "TEXT": "TEXT",
                    "VARCHAR": "TEXT",
                    "CHAR": "TEXT",
                    "BLOB": "BLOB",
                    "NUMERIC": "NUMERIC",
                    "DATE": "DATE",
                    "DATETIME": "DATETIME",
                    "TIMESTAMP": "TIMESTAMP",
                }

                schema = {}
                for row in rows:
                    col_name = row["name"].lower()
                    col_type = row["type"].upper() if row["type"] else "TEXT"

                    for sqlite_type, sql_type in type_mapping.items():
                        if sql_type in col_type:
                            col_type = sql_type
                            break
                    schema[col_name] = col_type
                return schema
        except Exception as e:
            logger.error(f"Database operation error: {e}")
            raise

    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """Get information about a table as a list of dictionaries."""
        query = f"PRAGMA table_info({table_name})"
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Database operation error: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        query = """
        SELECT name 
        FROM sqlite_master 
        WHERE type='table' AND name=?
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, (table_name,))
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Database operation error: {e}")
            raise

    def get_row_count(self, table_name: str) -> int:
        """Get the number of rows in a table."""
        query = f"SELECT COUNT(*) FROM {table_name}"
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                return result["COUNT(*)"] if result else 0
        except Exception as e:
            logger.error(f"Database operation error: {e}")
            raise

    def get_all_tables(self) -> List[str]:
        """Get a list of all tables in the database."""
        query = """SELECT name 
                 FROM sqlite_master WHERE type='table'
                 AND name NOT LIKE 'sqlite_%'
                 ORDER BY name
                 """
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query)
                return [row["name"] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Database operation error: {e}")
            return []

    def close(self):
        """Close the database connection."""
        if hasattr(self._local, "connection"):
            self._local.connection.close()
            self._local.connection = None
            logger.info("Database connection closed.")


def get_db_manager(db_path: Optional[str] = None) -> DatabaseManager:
    """Get the singleton database manager instance."""
    return DatabaseManager(db_path)
