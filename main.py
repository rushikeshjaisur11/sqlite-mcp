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
from typing import Optional

mcp = FastMCP("sqlite-mcp")
service = get_sqlite_service()


@mcp.tool
async def query_sqlite_table(
    user_text: str,
    table: str,
    rows_budget: int = MAX_ROWS_BUDGET,
    limit: int = MAX_PREVIESW_ROWS,
    db_path: Optional[str] = None,
) -> str:
    """Query a SQLite table.
    Provide table name in query or as a parameter"""

    request = MCPRequest(
        user_text=user_text,
        table=table,
        rows_budget=rows_budget,
        limit=limit,
        db_path=db_path,
    )
    return service.query_table(request)


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
async def test_connection(db_path: Optional[str] = None) -> str:
    """Test the connection to the database."""
    request = DataBaseConnectionRequest(db_path=db_path)
    return service.test_connection(request)


@mcp.tool
async def list_all_tables_with_path(db_path: Optional[str] = None) -> str:
    """List all tables in the database with optional database path."""
    return service.get_all_tables_with_path(db_path)


@mcp.tool
async def simple_query(sql: str, db_path: Optional[str] = None) -> str:
    """Execute a simple SQL query directly on the database."""
    try:
        from utils.database import get_db_manager

        db_manager = get_db_manager(db_path)
        results = db_manager.execute_query(sql)

        if not results:
            return "No results found."

        # Format results
        output = []
        if results:
            columns = list(results[0].keys())
            output.append(f"Columns: {', '.join(columns)}")
            output.append(f"Rows: {len(results)}")
            output.append("")

            for i, row in enumerate(results, 1):
                output.append(f"Row {i}: {dict(row)}")

        return "\n".join(output)
    except Exception as e:
        return f"Error: {e}"


if __name__ == "__main__":
    mcp.run(transport="stdio")
