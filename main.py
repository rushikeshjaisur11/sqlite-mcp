import asyncio
from fastmcp import FastMCP

from utils import (
    MAX_ROWS_BUDGET,
    MAX_PREVIESW_ROWS,
    get_sqlite_service,
    MCPRequest,
    TablePreviewRequest,
    ColumnStatsRequest,
    FindTablesRequest,
    TableSchemaRequest,
    DataBaseConnectionRequest,
)
from utils.config import set_database_path, get_database_path
from utils.database import update_db_manager_path

mcp = FastMCP("sqlite-mcp")
service = get_sqlite_service()


@mcp.tool
async def query_sqlite_table(
    user_text: str,
    table: str,
    rows_budget: int = MAX_ROWS_BUDGET,
    limit: int = MAX_PREVIESW_ROWS,
) -> str:
    """Query a SQLite table.
    Provide table name in query or as a parameter"""

    request = MCPRequest(
        user_text=user_text, table=table, rows_budget=rows_budget, limit=limit
    )
    return service.quert_table(request)


@mcp.tool
async def list_all_tables() -> str:
    """List all tables in the database."""
    return service.get_all_tables()


@mcp.tool
async def get_table_preview(table: str, limit: int = 10) -> str:
    """Get a preview of a table in the database."""
    request = TablePreviewRequest(table=table, limit=limit)
    return service.get_table_preview(request)


@mcp.tool
async def get_column_statistics(table: str, column: str = None) -> str:
    """Get statistics for a column in a table."""
    request = ColumnStatsRequest(table=table, column=column)
    return service.get_column_statistics(request)


@mcp.tool
async def find_tables_by_column(column_name: str) -> str:
    """Find tables that contain a specific column."""
    request = FindTablesRequest(column_name=column_name)
    return service.find_tables_by_column(request)


@mcp.tool
async def get_table_schema_info(table: str) -> str:
    """Get the schema information for a table."""
    request = TableSchemaRequest(table=table)
    return service.get_table_schema_info(request)


@mcp.tool
async def get_database_overview() -> str:
    """Get an overview of the database structure."""
    return service.get_database_overview()


@mcp.tool
async def test_connection() -> str:
    """Test the connection to the database."""
    request = DataBaseConnectionRequest()
    return service.test_connection(request)


@mcp.tool
async def set_database_file_path(database_path: str) -> str:
    """Set the database file path for the MCP server.

    Args:
        database_path: The path to the SQLite database file

    Returns:
        Confirmation message with the new database path
    """
    try:
        # Update the global configuration
        set_database_path(database_path)

        # Update the database manager with the new path
        update_db_manager_path(database_path)

        # Test the new connection
        request = DataBaseConnectionRequest()
        test_result = service.test_connection(request)

        return f"Database path successfully set to: {database_path}\n\nConnection test result:\n{test_result}"
    except Exception as e:
        return f"Error setting database path to '{database_path}': {str(e)}"


@mcp.tool
async def get_current_database_path() -> str:
    """Get the current database file path being used by the MCP server.

    Returns:
        The current database path
    """
    return f"Current database path: {get_database_path()}"


if __name__ == "__main__":
    mcp.run(transport="stdio")
