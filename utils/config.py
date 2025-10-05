import os
from typing import Optional

# Database configuration
DATABASE_PATH = os.getenv("DATABASE_PATH", "database.db")

# Query configuration
DEFAULT_LIMIT = 50
MAX_PREVIESW_ROWS = 50
DEFAULT_WINDOW_DAYS = 365
MAX_QUERY_TIMEOUT = 300  # in seconds

# Performance configuration
MAX_ROWS_BUDGET = 1000000  # Maximum number of rows to process in a query
SAMPLING_RATE = 0.01  # Sampling rate for large datasets

# Data column candidates

DATE_COLUMN_CANDIDATES = [
    "date",
    "created_at",
    "updated_at",
    "timestamp",
    "time",
    "datetime",
    "start_date",
    "end_date",
]

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(filename)s - %(levelname)s - %(message)s"


class DatabaseConfig:
    """Configuration class for database connection."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or DATABASE_PATH
        self.timeout = MAX_QUERY_TIMEOUT
        self.check_same_thread = False
        self.isolation_level = None  # Autocommit mode

    def set_database_path(self, db_path: str) -> None:
        """Set a new database path."""
        self.db_path = db_path

    @property
    def get_connection_string(self) -> str:
        """Returns the connection string for the database."""
        return self.db_path


# Global configuration instance - will be updated when database path is set
db_config = DatabaseConfig()


def set_database_path(db_path: str) -> None:
    """Set the global database path for the MCP server."""
    global db_config
    db_config.set_database_path(db_path)


def get_database_path() -> str:
    """Get the current database path."""
    return db_config.db_path
