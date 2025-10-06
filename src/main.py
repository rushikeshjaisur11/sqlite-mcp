import asyncio
from fastmcp import FastMCP

from src.utils import (
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


@mcp.tool
async def query_sqlite_table(
    user_text: str,
    table: Optional[str] = None,
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
    service = get_sqlite_service(db_path)
    return service.query_table(request)


@mcp.tool
async def list_all_tables(db_path: Optional[str] = None) -> str:
    """List all tables in the database."""
    service = get_sqlite_service(db_path)
    return service.get_all_tables()


@mcp.tool
async def get_table_preview(
    table: str, limit: int = 10, db_path: Optional[str] = None
) -> str:
    """Get a preview of a table in the database."""
    request = TablePreviewRequest(table=table, limit=limit)
    service = get_sqlite_service(db_path)
    return service.get_table_preview(request)


@mcp.tool
async def get_column_statistics(
    table: str, column: str = None, db_path: Optional[str] = None
) -> str:
    """Get statistics for a column in a table."""
    request = ColumnStatsRequest(table=table, column=column)
    service = get_sqlite_service(db_path)
    return service.get_column_statistics(request)


@mcp.tool
async def find_tables_by_column(column_name: str, db_path: Optional[str] = None) -> str:
    """Find tables that contain a specific column."""
    request = FindTablesRequest(column_name=column_name)
    service = get_sqlite_service(db_path)
    return service.find_tables_by_column(request)


@mcp.tool
async def get_table_schema_info(table: str, db_path: Optional[str] = None) -> str:
    """Get the schema information for a table."""
    request = TableSchemaRequest(table=table)
    service = get_sqlite_service(db_path)
    return service.get_table_schema_info(request)


@mcp.tool
async def get_database_overview(db_path: Optional[str] = None) -> str:
    """Get an overview of the database structure."""
    service = get_sqlite_service(db_path)
    return service.get_database_overview()


@mcp.tool
async def test_connection(db_path: Optional[str] = None) -> str:
    """Test the connection to the database."""
    request = DataBaseConnectionRequest(db_path=db_path)
    service = get_sqlite_service(db_path)
    return service.test_connection(request)


def main():
    """Starts MCP server"""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    mcp.run(transport="stdio")
