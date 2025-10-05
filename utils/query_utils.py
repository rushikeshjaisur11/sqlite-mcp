import re
import time
from datetime import date, timedelta
from typing import Any, Optional

from .config import DEFAULT_LIMIT, DEFAULT_WINDOW_DAYS, MAX_ROWS_BUDGET, SAMPLING_RATE
from .database import get_db_manager
from .logging import get_logger
from .models import MCPRequest, MCPResponse, QueryFilter
from .utils import (
    build_where_clause,
    estimate_query_cost,
    extract_query_components,
    find_date_column,
    normalize_table_ref,
)

logger = get_logger(__name__)


class QueryService:
    """Service for building and executing database queries."""

    def __init__(self):
        self._db_manager = get_db_manager()

    def _determine_table_ref(
        self, request: MCPRequest, query_filter: QueryFilter
    ) -> str:
        """Determine the full table reference from a given table name."""
        # For SQLite, the table name is usually sufficient
        if request.table:
            return normalize_table_ref(request.table)
        # Try to extract from user text
        match = re.search(r"from\s+([azA-Z0-9_\.]+)", request.user_text, re.IGNORECASE)
        if match:
            return match.group(1)
        raise ValueError("Table name could not be determined from the request.")

    def _build_sql_query(
        self,
        table_name: str,
        table_schema: dict,
        query_filter: QueryFilter,
        limit: int,
        row_count: int
    ) -> tuple[str, dict, list[str]]:
        """Build the SQL query based on the table schema and query filter."""
        auto_applied = []

        if query_filter.columns:
            valid_columns = [
                col for col in query_filter.columns if col.lower() in table_schema
            ]

        if not valid_columns:
            raise ValueError("No valid columns found in the query filter.")

        columns = valid_columns
        if not columns:
            columns = list(table_schema.keys())[:12]
            if len(table_schema) > 12:
                auto_applied.append("column_limit")

        date_column = find_date_column(table_schema)

        if (
            (not query_filter.start_date and not query_filter.end_date)
            and date_column
            and row_count > 10000
        ):
            end_date = date.today()
            start_date = end_date - timedelta(days=DEFAULT_WINDOW_DAYS)
            query_filter.start_date = start_date.isoformat()
            query_filter.end_date = end_date.isoformat()
            auto_applied.append(
                f"auto date window {start_date.isoformat()} to {end_date.isoformat()}"
            )

        # Build WHERE clause
        where_conditions, params = build_where_clause(
            query_filter.filters,
            table_schema,
            query_filter.start_date,
            query_filter.end_date,
            date_column,
        )

        select_clause = f"SELECT {', '.join(columns)} FROM {table_name}"
        from_clause = f" FROM {table_name}"
        where_clause = (
            f" WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        )

        # Add group by if specified
        if query_filter.group_by:
            valid_group_by = [
                col for col in query_filter.group_by if col.lower() in table_schema
            ]
            if valid_group_by:
                group_by_clause = f" GROUP BY {', '.join(valid_group_by)}"

        # Add order by if specified
        if query_filter.order_by:
            order_by_clause = f" ORDER BY {query_filter.order_by}"
        elif not group_by_clause:
            order_by_clause = f"ORDER BY {columns[0]}"

        # Add LIMIT
        limit_clause = f" LIMIT {limit}"
        params["limit"] = limit

        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        if group_by_clause:
            sql_parts.append(group_by_clause)
        if order_by_clause:
            sql_parts.append(order_by_clause)
        sql_parts.append(limit_clause)
        sql = " ".join(sql_parts)
        return sql, params, auto_applied

    def _add_sampling(self, sql: str, sampling_rate: float) -> str:
        """Add sampling to the given SQL query."""
        if "WHERE" in sql.upper():
            sql = sql.replace(
                "WHERE", f"WHERE ABS(RANDOM()) < {int(sampling_rate * 100)} AND ", 1
            )
        else:
            if "ORDER BY" in sql.upper():
                sql = sql.replace(
                    "ORDER BY",
                    f"WHERE ABS(RANDOM()) < {int(sampling_rate * 100)} ORDER BY ",
                    1,
                )
            elif "LIMIT" in sql.upper():
                sql = sql.replace(
                    "LIMIT",
                    f"WHERE ABS(RANDOM()) < {int(sampling_rate * 100)} LIMIT ",
                    1,
                )
            else:
                sql += f" WHERE ABS(RANDOM()) < {int(sampling_rate * 100)}"
        return sql

    def process_mcp_request(self, request: MCPRequest) -> MCPResponse:
        """Process a MCPRequest and return a MCPResponse."""
        start_time = time.time()
        try:
            # Extract query components
            query_filter = extract_query_components(request.user_text)

            # Determine table reference
            table_ref = self._determine_table_ref(request, query_filter)
            table_name = normalize_table_ref(table_ref)
            logger.info(f"Determined table reference: {table_name}")

            # Validate table existence and get schema
            if not self._db_manager.table_exists(table_name):
                return MCPResponse(
                    sql_preview="",
                    params={},
                    auto_applied=[],
                    note=f"Table '{table_name}' does not exist.",
                )
            table_schema = self._db_manager.get_table_schema(table_name)
            row_count = self._db_manager.get_table_row_count(table_name)

            # Build SQL query

            sql, params, auto_applied = self._build_sql_query(
                table_name,
                table_schema,
                query_filter,
                request.limit or DEFAULT_LIMIT,
                row_count or MAX_ROWS_BUDGET
            )

            # Estimate query cost
            where_conditions = []
            if "WHERE" in sql:
                where_part = (
                    sql.split("WHERE", 1)[1].split("ORDER BY", 1)[0]
                    if "ORDER BY" in sql
                    else sql.split("WHERE", 1)[1]
                )
                where_conditions = [where_part.strip()]
            estimated_rows = estimate_query_cost(
                table_name, where_conditions, row_count
            )

            if estimated_rows <= (request.rows_budget or MAX_ROWS_BUDGET):
                try:
                    rows = self._db_manager.execute_query(sql, params)
                    execution_time = time.time() - start_time
                    return MCPResponse(
                        sql_preview=sql,
                        params=params,
                        auto_applied=auto_applied,
                        rows=rows,
                        estimated_rows=estimated_rows,
                        execution_time=execution_time,
                    )
                except Exception as e:
                    logger.error(f"Error executing query: {e}")
                    return MCPResponse(
                        sql_preview=sql,
                        params=params,
                        auto_applied=auto_applied,
                        note=f"Error executing query: {e}",
                    )
            else:

                # Try sampling for large queries
                sampling_sql = self._add_sampling(sql, SAMPLING_RATE)
                sampling_estimated = int(estimated_rows * SAMPLING_RATE)

                if sampling_estimated <= (rows.budget or MAX_ROWS_BUDGET):
                    try:
                        rows = self._db_manager.execute_query(sampling_sql, params)
                        execution_time = time.time() - start_time
                        return MCPResponse(
                            sql_preview=sampling_sql,
                            params=params,
                            auto_applied=auto_applied + ["sampling"],
                            rows=rows,
                            estimated_rows=sampling_estimated,
                            execution_time=execution_time,
                        )
                    except Exception as e:
                        logger.error(f"Error executing sampling query: {e}")
                        return MCPResponse(
                            sql_preview=sampling_sql,
                            params=params,
                            auto_applied=auto_applied + ["sampling"],
                            note=f"Error executing sampling query: {e}",
                        )

        except Exception as e:
            logger.error(f"Error processing request: {e}")
            return MCPResponse(
                sql_preview="",
                params={},
                auto_applied=[],
                note=f"Error processing request: {e}",
            )

    def execute_raw_query(
        self, sql: str, params: Optional[dict] = None
    ) -> list[dict[str, Any]]:
        """Execute a raw SQL query and return the response."""
        return self._db_manager.execute_query(sql, params or {})


query_service = QueryService()


def get_query_service() -> QueryService:
    """Get the QueryService instance."""
    return query_service
