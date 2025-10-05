from .database import get_db_manager
from .exploration import get_exploration_service
from .models import (
    MCPRequest,
    TablePreviewRequest,ColumnStatsRequest, FindTablesRequest, TableSchemaRequest,DatabaseConectionRequest,ErrorResponse
)
from .query_utils import get_query_service

class SQLiteService:
    """Service for interacting with SQLite database."""

    def __init__(self):
        self.exploration_service = get_exploration_service()
        self.query_service = get_query_service()
    
    def quert_table(self,request:MCPRequest) -> str:
        try:
            response = self.query_service.process_mcp_request(request)
            
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
            if hasattr(response,'estimated_rows'):
                result_parts.append(f"Estimated Rows: {response.estimated_rows:,}")
            
            # Add results 
            if response.rows:
                result_parts.append(f"Rows: {len(response.rows)}")
                for i,row in enumerate(response.rows[:5],start=1):
                    result_parts.append(f"Row {i}: {dict(row)}")
                if len(response.rows) > 5:
                    result_parts.append(f"... and {len(response.rows) -5} more rows")
                
            # Add notes 
            if response.note:
                result_parts.append(f"Note: {response.note}")
            
            # Add execution time
            if response.execution_time:
                result_parts.append(f"Execution Time: {response.execution_time:.2f} seconds")
            
            return '\n'.join(result_parts)
            
        except Exception as e:
            logger.error(f"Error querying table: {e}")
            return f"Error querying table: {e}"
    
    def get_table_preview(self,request:TablePreviewRequest) -> str:
        """Get a preview of a table in the database."""
        try:
            return self.exploration_service.get_table_preview(request.table,request.limit)
        except Exception as e:
            error_response = ErrorResponse(error=f"Error getting table preview",details=str(e),success=False)
            return f"Error: {error_response.error} - {error_response.details}"
        
    def get_column_statistics(self,request:ColumnStatsRequest)->str:
        """Get statistics for a column in a table."""
        try:
            return self.exploration_service.get_column_statistics(request.table,request.column)
        except Exception as e:
            error_response = ErrorResponse(error=f"Error getting column statistics",details=str(e),success=False)
            return f"Error: {error_response.error} - {error_response.details}"
        
    def find_tables_by_column(self,request:FindTablesRequest) -> str:
        """Find tables that contain a specific column."""
        try:
            return self.exploration_service.find_tables_by_column(request.column_name)
        except Exception as e:
            error_response = ErrorResponse(error=f"Error finding tables by column",details=str(e),success=False)
            return f"Error: {error_response.error} - {error_response.details}"
        
    def get_all_tables(self) -> str:
        """Get a list of all tables in the database."""
        try:
            return self.exploration_service.get_all_tables()
        except Exception as e:
            error_response = ErrorResponse(error=f"Error getting all tables",details=str(e),success=False)
            return f"Error: {error_response.error} - {error_response.details}"
        
    