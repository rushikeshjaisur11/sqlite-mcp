from typing import Optional
from .database import get_db_manager
from .exploration import get_exploration_service
from .logging import get_logger
from .models import (
    MCPRequest,
    TablePreviewRequest,
    ColumnStatsRequest,
    FindTablesRequest,
    TableSchemaRequest,
    ErrorResponse,
    DataBaseConnectionRequest,
)
from .query_utils import get_query_service

logger = get_logger(__name__)


class SQLiteService:
    """Service for interacting with SQLite database."""

    def __init__(self, db_path: Optional[str] = None):
        self.exploration_service = get_exploration_service(db_path)
        self.query_service = get_query_service(db_path)

    def query_table(self, request: MCPRequest) -> str:
        try:
            query_service = get_query_service(request.db_path)
            response = query_service.process_mcp_request(request)

            result_parts = []

            if response.sql_preview:
                result_parts.append(f"SQL Query: {response.sql_preview}")

            # Add parameters if any
            if response.params:
                result_parts.append(f"Parameters: {response.params}")

            # Add auto-applied filters
            if response.auto_applied:
                result_parts.append(f"Auto Applied: {', '.join(response.auto_applied)}")

            # Add estimated cost
            if hasattr(response, "estimated_rows"):
                result_parts.append(f"Estimated Rows: {response.estimated_rows:,}")

            # Add results
            if response.rows:
                result_parts.append(f"Rows: {len(response.rows)}")
                for i, row in enumerate(response.rows[:5], start=1):
                    result_parts.append(f"Row {i}: {dict(row)}")
                if len(response.rows) > 5:
                    result_parts.append(f"... and {len(response.rows) -5} more rows")

            # Add notes
            if response.note:
                result_parts.append(f"Note: {response.note}")

            # Add execution time
            if response.execution_time:
                result_parts.append(
                    f"Execution Time: {response.execution_time:.2f} seconds"
                )

            return "\n".join(result_parts)

        except Exception as e:
            logger.error(f"Error querying table: {e}")
            return f"Error querying table: {e}"

    def get_table_preview(self, request: TablePreviewRequest) -> str:
        """Get a preview of a table in the database."""
        try:
            return self.exploration_service.get_table_preview(
                request.table, request.limit
            )
        except Exception as e:
            error_response = ErrorResponse(
                error=f"Error getting table preview", details=str(e), success=False
            )
            return f"Error: {error_response.error} - {error_response.details}"

    def get_column_statistics(self, request: ColumnStatsRequest) -> str:
        """Get statistics for a column in a table."""
        try:
            return self.exploration_service.get_column_statistics(
                request.table, request.column
            )
        except Exception as e:
            error_response = ErrorResponse(
                error=f"Error getting column statistics", details=str(e), success=False
            )
            return f"Error: {error_response.error} - {error_response.details}"

    def find_tables_by_column(self, request: FindTablesRequest) -> str:
        """Find tables that contain a specific column."""
        try:
            return self.exploration_service.find_tables_by_column(request.column_name)
        except Exception as e:
            error_response = ErrorResponse(
                error=f"Error finding tables by column", details=str(e), success=False
            )
            return f"Error: {error_response.error} - {error_response.details}"

    def get_all_tables(self) -> str:
        """Get a list of all tables in the database."""
        try:
            return self.exploration_service.get_all_tables()
        except Exception as e:
            error_response = ErrorResponse(
                error=f"Error getting all tables", details=str(e), success=False
            )
            return f"Error: {error_response.error} - {error_response.details}"

    def get_table_schema_info(self, request: TableSchemaRequest) -> str:
        """Get the schema information for a table."""
        try:
            return self.exploration_service.get_table_schema_info(request.table)
        except Exception as e:
            error_response = ErrorResponse(
                error=f"Error getting table schema info", details=str(e), success=False
            )
            return f"Error: {error_response.error} - {error_response.details}"

    def get_database_overview(self) -> str:
        """Get an overview of the database structure."""
        try:
            return self.exploration_service.get_database_overview()
        except Exception as e:
            error_response = ErrorResponse(
                error=f"Error getting database overview", details=str(e), success=False
            )
            return f"Error: {error_response.error} - {error_response.details}"

    def test_connection(self, request: DataBaseConnectionRequest) -> str:
        """Test the connection to the database."""
        try:
            db_manager = get_db_manager(request.db_path)
            with db_manager.get_cursor() as cursor:
                cursor.execute("select sqlite_version()")
                version = cursor.fetchone()
                # Get Database file info
                cursor.execute("PRAGMA database_list")
                db_info = cursor.fetchone()
                # Get table content
                cursor.execute(
                    "select count(*) as count from sqlite_master where type = 'table'"
                )
                table_count = cursor.fetchone()

                cursor.close()

                return f"""Connection Successfull !!!!
            SQLite Version : {version['sqlite_version()']}
            Database : {db_info['file'] if db_info else 'N/A'}
            Tables : {table_count['count'] if table_count else '0'}"""
        except Exception as e:
            error_response = ErrorResponse(
                error=f"Error testing connection", details=str(e), success=False
            )
            return f"Error: {error_response.error} - {error_response.details}"

    def get_all_tables_with_path(self, db_path: Optional[str] = None) -> str:
        """Get a list of all tables in the database with optional db_path."""
        try:
            if db_path:
                exploration_service = get_exploration_service(db_path)
                return exploration_service.get_all_tables()
            else:
                return self.exploration_service.get_all_tables()
        except Exception as e:
            error_response = ErrorResponse(
                error=f"Error getting all tables", details=str(e), success=False
            )
            return f"Error: {error_response.error} - {error_response.details}"


def get_sqlite_service(db_path: Optional[str] = None) -> SQLiteService:
    """Get the SQLite service instance."""
    return SQLiteService(db_path)
