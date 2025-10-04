import os

# Database configuration
DATABASE_PATH = os.getenv("DATABASE_PATH", "database.db")

# Query configuration
DEFAULT_LIMIT = 50
MAX_PREVIESW_ROWS = 50
DEFAULT_WINDOW_DAYS = 365
MAX_QUERY_TIMEOUT = 300  # in seconds

# Performance configuration
MAX_ROWS_BUDGET = 1e6  # Maximum number of rows to process in a query
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
    """Configuration class for database connection.
    """

    def __init__(self, db_path: str = DATABASE_PATH):
        self.db_path = db_path
        self.timeout = MAX_QUERY_TIMEOUT
        self.check_same_thread = False
        self.isolation_level = None  # Autocommit mode

    @property
    def get_connection_string(self) -> str:
        """Returns the connection string for the database.
        """
        return self.db_path


db_config = DatabaseConfig()
