from typing import Optional
from .logging import get_logger

from .database import get_db_manager
from .models import MCPRequest
from .query_utils import get_query_service
from .utils import normalize_table_ref, format_query_results

logger = get_logger(__name__)


class ExplorationService:
    """Service for exploring SQLite database structure and data"""

    def __init__(self, db_path: Optional[str] = None):
        self.db_manager = get_db_manager(db_path)

    def get_table_preview(self, table_ref: str, limit: int = 10) -> str:
        """Get a preview of a table in the database.

        Args:
            table_ref (str): The name of the table to preview.
            limit (int, optional): The maximum number of rows to return. Defaults to 10.

        Returns:
            str: A string representation of the table preview.
        """
        try:
            table = normalize_table_ref(table_ref)
            if not self.db_manager.table_exists(table):
                return f"Table '{table}' does not exist."

            table_schema = self.db_manager.get_table_schema(table)
            columns = list(table_schema.keys())[:10]
            cols_sql = ", ".join(columns)
            query = f"SELECT {cols_sql} FROM {table} ORDER BY 1 LIMIT :limit"

            rows = self.db_manager.execute_query(query, {"limit": limit})
            result = (
                f"Preview of table '{table} "
                f"Showing {len(columns)} columns and {len(rows)}"
            )
            if rows:
                result += "Columns: " + ", ".join(columns) + "\n\n"
                for i, row in enumerate(rows, start=1):
                    result += f"Row {i}:{dict(row)}\n"
            else:
                result += "No rows found."
            return result

        except Exception as e:
            logger.error(f"Error getting table preview: {e}")
            return f"Error getting table preview: {e}"

    def get_column_statistics(
        self, table_ref: str, column: Optional[str] = None
    ) -> str:
        """Get statistics for a column in a table.

        Args:
            table_ref: Table reference
            column_name: Specific of column name (optional)
        Returns:
            Formatted statistics string
        """

        try:
            table = normalize_table_ref(table_ref)

            if not self.db_manager.table_exists(table):
                return f"Table '{table}' does not exist."

            table_schema = self.db_manager.get_table_schema(table)
            if column:
                if column.lower() not in table_schema:
                    return f"Column '{column}' does not exist in table '{table}'."
                return self._get_single_column_stats(table, column, table_schema)
            else:
                return self._get_all_column_stats(table, table_schema)
        except Exception as e:
            logger.error(f"Error getting column statistics: {e}")
            return f"Error getting column statistics: {e}"

    def _get_single_column_stats(
        self, table: str, column: str, table_schema: dict
    ) -> str:
        """Get statistics for a single column in a table."""

        data_type = table_schema[column.lower()].upper()
        numeric_types = ["INTEGER", "REAL", "NUMERIC"]

        if any(dt in data_type for dt in numeric_types):
            query = f"""
                SELECT 
                    COUNT(*) AS total_rows,
                    COUNT({column}) AS non_null_count,
                    COUNT(*) - COUNT({column}) AS null_count,
                    MIN({column}) AS min_value,
                    MAX({column}) AS max_value,
                    AVG({column}) AS avg_value,
                    SUM({column}) AS sum
                FROM {table}
            """
        else:
            query = f"""
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT({column}) as non_null_count,
                    COUNT(*) - COUNT({column}) AS null_count,
                    COUNT(DISTINCT {column}) AS distinct_count
                FROM {table}
            """
        rows = self.db_manager.execute_query(query)

        if not rows:
            return f"No statistics found for column '{column}' in table '{table}'."

        stats = rows[0]
        result = f"Statistics for column '{column}' in table '{table}':\n"
        result += f"  Data Type: {data_type}\n"
        for key, value in stats.items():
            if value is not None:
                if isinstance(value, float):
                    result += f"  {key} : {value:.2f}\n"
                else:
                    result += f"  {key}: {value}\n"
        return result

    def _get_all_column_info(self, table: str, table_schema: dict) -> str:
        """Get info for all columns in a table."""

        table_info = self.db_manager.get_table_info(table)
        result = f"Column Information for table '{table}':\n"
        for col_info in table_info:
            result += f"    {col_info['name']}: ({col_info['type'] or "TEXT"})\n"

            if col_info["notnull"] == 1:
                result += " (NOT NULL)"
            else:
                result += " (nullable)"

            if col_info["dflt_value"]:
                result += f" default: {col_info['dflt_value']}\n"

            result += "\n"

        return result

    def find_tables_by_column(self, column_name: str) -> str:
        """Find tables that contain a specific column."""

        try:
            tables = self.db_manager.get_all_tables()
            matches = []

            for table in tables:
                try:
                    table_schema = self.db_manager.get_table_schema(table)
                    if column_name.lower() in table_schema:
                        data_type = table_schema[column_name.lower()].upper()
                        matches.append(f"{table} ({data_type})")
                except Exception as e:
                    logger.warning(f"Failed to check table : {table} : {e}")

            if not matches:
                return f"No tables found with column '{column_name}'."

            result = f"Tables containing column '{column_name}':\n"
            for table, col, dtype in matches:
                result += f"  {table} - {col} - {dtype}\n"
            return result
        except Exception as e:
            logger.error(f"Error finding tables by column: {e}")
            return f"Error finding tables by column: {e}"

    def get_database_overview(self) -> str:
        """Get an overview of the database structure."""

        try:
            tables = self.db_manager.get_all_tables()

            if not tables:
                return "No tables found in the database."

            result = f"Database overview - Found {len(tables)} tables.\n\n"
            for table in tables:
                try:
                    row_count = self.db_manager.get_row_count(table)
                    table_schema = self.db_manager.get_table_schema(table)
                    col_count = len(table_schema)
                    result += f"   - {table}: {row_count} rows, {col_count} columns\n"
                except Exception as e:
                    logger.warning("Failed to get info for table {table}: {e}")
                    result += f"   - {table}: (info unavailable)\n"
            return result
        except Exception as e:
            logger.error(f"Error getting database overview: {e}")
            return f"Error getting database overview: {e}"

    def get_all_tables(self) -> str:
        """Get a list of all tables in the database."""

        try:
            tables = self.db_manager.get_all_tables()
            if not tables:
                return "No tables found in the database."
            result = "Tables in the database:\n"
            for table in tables:
                result += f"  - {table}\n"
            return result
        except Exception as e:
            logger.error(f"Error getting all tables: {e}")
            return f"Error getting all tables: {e}"

    def get_table_schema_info(self, table_ref: str) -> str:
        """Get the schema information for a table."""
        try:
            table = normalize_table_ref(table_ref)
            if not self.db_manager.table_exists(table):
                return f"Table '{table}' does not exist."

            table_schema = self.db_manager.get_table_schema(table)
            result = f"Schema information for table '{table}':\n"
            for col, dtype in table_schema.items():
                result += f"  {col}: {dtype}\n"

            row_count = self.db_manager.get_row_count(table)
            result += f"\nTotal Rows : {row_count:,}"

            return result
        except Exception as e:
            logger.error(f"Error getting table schema info: {e}")
            return f"Error getting table schema info: {e}"

    def query_table_data(
        self,
        user_text: str,
        table_ref: Optional[str] = None,
        rows_budget: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> str:
        """Query data from a table."""

        try:
            request = MCPRequest(
                user_text=user_text,
                table=table_ref,
                rows_budget=rows_budget,
                limit=limit,
            )
            query_service = get_query_service()
            response = query_service.process_mcp_request(request)

            result = f"SQL Query: {response.sql_preview}\n"

            if response.estimated_rows:
                result += f"Estimated Rows: {response.estimated_rows:,}\n"

            if response.auto_applied:
                result += f"Auto Applied: {', '.join(response.auto_applied)}\n"
            if response.execution_time:
                result += f"Execution Time: {response.execution_time:.2f} seconds\n"

            result += "\n"

            if response.rows:
                results += format_query_results(response.rows, limit or 10)
            elif response.note:
                result += f"Note: {response.note}\n"

            return result
        except Exception as e:
            logger.error(f"Error querying table data: {e}")
            return f"Error querying table data: {e}"


def get_exploration_service(db_path: Optional[str] = None) -> ExplorationService:
    """Get the exploration service instance."""
    if db_path:
        return ExplorationService(db_path)
    return ExplorationService()
