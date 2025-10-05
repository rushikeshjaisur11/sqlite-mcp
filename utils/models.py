from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class MCPRequest(BaseModel):
    """Request model for mcp queries.
    """

    user_text: str = Field(
        ..., description="The query input from the user in natural langauge."
    )
    table: str = Field(..., description="The table to query.")
    rows_budget: Optional[int] = Field(
        10, description="The number of rows to return from the table."
    )
    limit: Optional[int] = Field(
        5, description="The number of results to return from the query."
    )


class MCPResponse(BaseModel):
    """Response model for mcp queries.
    """

    sql_preview: str = Field(
        ..., description="The SQL query generated from the user input."
    )
    params: Dict[str, Any] = Field(..., description="The parameters for the SQL query.")
    estimated_rows: Optional[int] = Field(
        None, description="The estimated number of rows the SQL query will return."
    )
    auto_applied: List[str] = Field(
        ..., description="The list of auto applied filters."
    )
    rows: Optional[List[Dict[str, Any]]] = Field(
        None, description="The rows returned from the SQL query."
    )
    note: Optional[str] = Field(None, description="A note about the query.")
    execution_time: Optional[float] = Field(
        None, description="The time taken to execute the query in seconds."
    )


class QueryFilter(BaseModel):
    """Model for a filter applied to a query."""

    columns: List[str] = Field(default_factory=list, description="The columns the filter is applied to.")
    filters: Dict[str, Any] = Field(
        default_factory=dict, description="A mapping of column names to filter values."
    )
    start_date: Optional[str] = Field(
        None, description="The start date for date range filters."
    )
    end_date: Optional[str] = Field(
        None, description="The end date for date range filters."
    )
    order_by: Optional[str] = Field(
        None, description="The column to order the results by."
    )
    group_by: List[str] = Field(
        default_factory=list, description="The columns to group the results by."
    )


class TablePreviewRequest(BaseModel):
    """Request model for table preview.
    """

    table: str = Field(..., description="The table to preview.")
    limit: int = Field(5, description="The number of rows to return from the table.")


class ColumnStatsRequest(BaseModel):
    """Request model for column statistics.
    """

    table: str = Field(..., description="The table to get column statistics from.")
    column: Optional[str] = Field(..., description="The column to get statistics for.")


class FindTablesRequest(BaseModel):
    """Request model for finding tables.
    """

    column_name: str = Field(
        ..., description="The column name to search for in the database schema."
    )


class TableSchemaRequest(BaseModel):
    """Request model for table schema.
    """

    table: str = Field(..., description="The table to get the schema for.")


class DataBaseConnectionRequest(BaseModel):
    """Request model for database connection.
    """

    pass  # No fields required for this request


class ErrorResponse(BaseModel):
    """Response model for errors.
    """

    error: str = Field(..., description="The error message.")
    details: Optional[str] = Field(
        None, description="Additional details about the error."
    )
    success: bool = Field(
        False, description="Indicates if the operation was successful."
    )
