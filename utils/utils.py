import re
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from .config import DATE_COLUMN_CANDIDATES

from .logging import get_logger
from .models import QueryFilter

logger = get_logger(__name__)

def normalize_table_ref(table_ref: str) -> str:
    """Normalize table reference by removing special characters and converting to lowercase.

    Args:
        table_ref (str): The table reference to normalize

    Returns:
        table name (str): The normalized table name

    """
    parts = table_ref.split(".")
    if len(parts) >= 2:
        return parts[-1]
    return table_ref.lower().strip()


def extract_query_components(user_text:str)-> QueryFilter:
    """Extract query components from user input.

    This functions parses user text to extract
     - column selection (SELECT clause)
     - filters (WHERE clause)
     - date ranges (if any)
     - order by clauses (ORDER BY)
     - group by clauses (GROUP BY).
    """
    result = QueryFilter()

    # Extract SELECT clause
    select_match = re.search(r"select\s+([a-zA-Z0-9_,\s*]+)\s+from", user_text, re.IGNORECASE)
    if select_match:
        cols = [col.strip() for col in select_match.group(1).split(",") if col.strip() and col.strip() != "*"]
        result.columns = cols
        logger.debug(f"Extracted columns: {result.columns}")

    # Extract table reference
    table_match = re.search(r"from\s+([a-zA-Z0-9_.]+)", user_text, re.IGNORECASE)
    if table_match:
        # This will be handled by calling function.
        pass

    # Extract IN clauses
    for match in re.finditer(r"([a-zA-Z0-9_]+)\s+in\s*\(([^)]+)\)", user_text, re.IGNORECASE):
        col = match.group(1).strip().lower()
        vals = [v.strip().strip("'\"") for v in match.group(2).split(",") if v.strip()]
        result.filters[col] = vals
        logger.debug(f"Extracted IN clause for column {col}: {vals}")

    # Extract equality filters
    for match in re.finditer(r"([a-zA-Z0-9_]+)\s*=\s*([^\s,\)]+)", user_text, re.IGNORECASE):
        col = match.group(1).strip().lower()
        val = match.group(2).strip().strip("'\"")
        # Skip SQL keywords
        if col.lower() in ("from","select","where","limit","order","group","by"):
            continue
        logger.debug(f"Extracted equality filter for column {col}: {val}")
        # Prefer IN clause values if both exist
        if col not in result.filters:
            result.filters[col] = val

    # Extract like patterns : col LIKE 'pattern%'
    for match in re.finditer(r'([a-zA-Z0-9_]+)\s+like\s*[\'"]([^"\']+)[\'"]', user_text, re.IGNORECASE):
        col = match.group(1).strip().lower()
        pattern = match.group(2).strip()
        result.filters[f"{col}_like"] = pattern
        logger.debug(f"Extracted LIKE clause for column {col}: {pattern}")

    # Extract date ranges
    date_range_match = re.search(r'([a-zA-Z0-9_]+)\s+between\s+[\'"]([^"\']+)[\'"]\s+and\s+[\'"]([^"\']+)[\'"]', user_text, re.IGNORECASE)

    if date_range_match:
        result.start_date = date_range_match.group(1).strip()
        result.end_date = date_range_match.group(2).strip()
        logger.debug(f"Extracted date range: {result.start_date} to {result.end_date}")
    else:
        last_days_match = re.search(r"last\s+(\d+)\s+days", user_text, re.IGNORECASE)
        if last_days_match:
            n = int(last_days_match.group(1))
            result.start_date = (date.today() - timedelta(days=n)).isoformat()
            result.end_date = date.today().isoformat()
            logger.debug(f"Extracted last {n} days date range: {result.start_date} to {result.end_date}")
        else:
            if re.search(r"this\s+week", user_text, re.IGNORECASE):
                today = date.today()
                start_of_week = today - timedelta(days=today.weekday())
                result.start_date = start_of_week.isoformat()
                result.end_date = today.isoformat()
                logger.debug(f"Extracted this week date range: {result.start_date} to {result.end_date}")
            elif re.search(r"this\s+month", user_text, re.IGNORECASE):
                today = date.today()
                start_of_month = today.replace(day=1)
                result.start_date = start_of_month.isoformat()
                result.end_date = today.isoformat()
                logger.debug(f"Extracted this month date range: {result.start_date} to {result.end_date}")

    # Extract ORDER BY clause
    order_match = re.search(r"order\s+by\s+([a-zA-Z0-9_,\s*]+)(\s+asc|\s+desc)?", user_text, re.IGNORECASE)
    if order_match:
        col = order_match.group(1).strip()
        direction = order_match.group(2).strip().upper() if order_match.group(2) else "ASC"
        result.order_by = f"{col} {direction}"
        logger.debug(f"Extracted ORDER BY clause: {result.order_by}")

    # Extract GROUP BY clause
    group_match = re.search(r"group\s+by\s+([a-zA-Z0-9_,\s*]+)", user_text, re.IGNORECASE)
    if group_match:
        cols = [col.strip() for col in group_match.group(1).split(",") if col.strip()]
        result.group_by = cols
        logger.debug(f"Extracted GROUP BY clause: {result.group_by}")

    return result

def find_date_column(schema:Dict[str,str]) -> Optional[str]:
    """Find a date or timestamp column in the given schema.

    Args:
        schema (Dict[str,str]): A dictionary mapping column names to their data types.

    Returns:
        Optional[str]: The name of the date or timestamp column, or None if not found.

    """
    schema_lower = {k.lower(): v.lower() for k, v in schema.items()}
    for candidate in DATE_COLUMN_CANDIDATES:
        if candidate in schema_lower:
            data_type = schema_lower[candidate].upper()
            if any(dt in data_type for dt in ("DATE", "TIMESTAMP", "DATETIME")):
                return candidate
    return None

def build_where_clause(filters:Dict[str,Any],schema:Dict[str,str],
                       start_date:Optional[str]=None,
                       end_date:Optional[str]=None,
                       date_column:Optional[str]=None)-> Tuple[List[str],Dict[str,Any]]:
    """Build SQL WHERE clause and parameters from filters and date range.

    Args:
        filters (Dict[str,Any]): A dictionary of column filters.
        schema (Dict[str,str]): A dictionary mapping column names to their data types.
        start_date (Optional[str], optional): Start date for date range filter. Defaults to None.
        end_date (Optional[str], optional): End date for date range filter. Defaults to None.
        date_column (Optional[str], optional): The name of the date column to use for date range filtering. 
                                               If None, the function will attempt to find one in the schema. Defaults to None.

    Returns:
        Tuple[List[str],Dict[str,Any]]: A tuple containing the list of WHERE clause
                                            conditions and a dictionary of parameters.

    """
    where_clauses = []
    params = {}
    param_counter = 1

    # Add date range filter if specified
    if start_date and end_date and date_column:
        where_clauses.append(f"{date_column} BETWEEN :start_date AND :end_date")
        params["start_date"] = start_date
        params["end_date"] = end_date

    # Process other filters
    for key,value in filters.items():
        if key.endswith("_like"):
            for col_name in schema:
                param_name = f"param{param_counter}"
                where_clauses.append(f"{col_name} LIKE :{param_name}")
                params[param_name] = value
                param_counter += 1
        elif isinstance(value, list):
            # IN clause
            if key in schema:
                param_name = f"param{param_counter}"
                placeholders = ", ".join([f":{param_name}_{i}" for i in range(len(value))])
                where_clauses.append(f"{key} IN ({placeholders})")
                for i, val in enumerate(value):
                    params[f"{param_name}_{i}"] = val
                param_counter += 1
        else:
            # Equality filter
            if key in schema:
                param_name = f"param{param_counter}"
                data_type = schema[key].upper()
                if data_type in ("INTEGER","INT") and key.startswith("is_"):
                    bool_value = str(value).lower() in ("1","true","yes","on","y","t")
                    where_clauses.append(f"{key} = :{param_name}")
                    params[param_name] = 1 if bool_value else 0
                else:
                    where_clauses.append(f"{key} = :{param_name}")
                    params[param_name] = value
                param_counter += 1
    return where_clauses, params

def estimate_query_cost(table_name:str,where_conditions:List[str],row_count:int)-> float:
    """Estimate the cost of a SQL query based on table name, where conditions, and row count.

    Args:
        table_name (str): The name of the table being queried.
        where_conditions (List[str]): A list of WHERE clause conditions.
        row_count (int): The estimated number of rows in the table.

    Returns:
        float: An estimated cost value for the query.

    """
    if not where_conditions:
        return row_count
    for condition in where_conditions:
        if "=" in condition:
            estimated_rows = max(1, row_count // 10)
        elif "IN" in condition:
            estimated_rows = max(1, row_count // 5)
        elif "LIKE" in condition:
            estimated_rows = max(1, row_count // 2)
        else:
            estimated_rows = row_count
    return estimated_rows

def format_query_results(rows:List[Dict[str,Any]],limit:int=10)-> str:
    """Format SQL query results into a readable string.

    Args:
        rows (List[Dict[str,Any]]): A list of dictionaries representing query result rows.
        limit (int, optional): The maximum number of rows to include in the output. Defaults to 10.

    Returns:
        str: A formatted string representation of the query results.

    """
    if not rows:
        return "No results found."

    results = f"Reults {len(rows)} rows:\n\n"

    if rows:
        columns = list(rows[0].keys())
        results += "Columns: " + ", ".join(columns) + "\n\n"

    display_rows = rows[:limit]
    for i, row in enumerate(display_rows, start=1):
        results += f"Row {i}:{dict(row)}\n"
    if len(rows) > limit:
        results += f"... and {len(rows) - limit} more rows.\n"
    return results

def sanitize_identifier(identifier: str) -> str:
    """Sanitize SQL identifier by removing special characters and converting to lowercase.

    Args:
        identifier (str): The SQL identifier to sanitize.

    Returns:
        str: The sanitized SQL identifier.

    """
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "", identifier)

    if sanitized and sanitized[0].isdigit():
        sanitized = "_" + sanitized
    return sanitized.lower().strip()

def parse_limit_from_text(user_text: str, default_limit: int = 10) -> int:
    """Parse LIMIT value from user text.

    Args:
        user_text (str): The user input text.
        default_limit (int, optional): The default limit to return if none found. Defaults to 10.

    Returns:
        int: The parsed LIMIT value.

    """
    limit_match = re.search(r"limit\s+(\d+)", user_text, re.IGNORECASE)
    if limit_match:
        return min(int(limit_match.group(1)), 100)  # Cap at 100

    top_match = re.search(r"(?:top|first)\s+(\d+)", user_text, re.IGNORECASE)
    if top_match:
        return min(int(top_match.group(1)), 100)  # Cap at 100
    return default_limit

