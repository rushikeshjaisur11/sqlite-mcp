# SQLite-MCP

**A Model Context Protocol (MCP) server for interacting with SQLite databases.**

This server provides a set of tools to query and explore SQLite databases through a command-line or other MCP-compatible clients.

## Installation

### Local Installation

This project uses `uv` for dependency management. Make sure you have `uv` installed before proceeding.

1.  **Clone the repository:**
    ```bash
    ## SQLite-MCP

    A small Model Context Protocol (MCP) server for exploring and querying SQLite databases. Use it locally from the command line, wire it into Claude Desktop / VS Code, or run a lightweight HTTP wrapper with `uv`/`uvicorn`.

    Key features:
    - Query tables using natural-language prompts (translated to SQL).
    - Preview table rows and get column statistics.
    - Search for tables by column name and inspect schema information.

    ---

    ## Quick start (Windows - cmd.exe)

    1. Clone the repository and change directory:

    ```cmd
    git clone https://github.com/rushikeshjaisur11/sqlite-mcp.git
    cd sqlite-mcp
    ```

    2. Run the MCP server with `uv` (the project uses `uv` to manage / run MCP services):

    ```cmd
    uv run sqlite-mcp
    ```

    This starts the MCP server and listens for MCP calls over stdio.

    If you prefer an HTTP wrapper (FastAPI + uvicorn), see "HTTP API" below.

    ---

    ## Prerequisites

    - Python 3.10+ (or the version used by the project).
    - `uv` (used to run the MCP server). If you don't have `uv`, install it per the tool's docs.
    - For the optional HTTP API: `fastapi` and `uvicorn`.

    Install optional Python dependencies:

    ```cmd
    python -m pip install fastapi uvicorn
    ```

    ---

    ## Usage

    There are three common integration patterns you can use:

    1. CLI — call the core service functions directly from a simple script.
    2. MCP — run the project with `uv run sqlite-mcp` to expose MCP tools.
    3. HTTP API — wrap the same service with FastAPI and run with `uvicorn`.

    Below are examples and notes for each.

    ### CLI (local scripts)

    Create a tiny `cli.py` that imports the project's service functions (e.g. from `utils.sqllite_service`) and calls them. Example usage from `cmd.exe`:

    ```cmd
    python cli.py set-db "C:\path\to\database.db"
    python cli.py get-db
    python cli.py test-conn
    ```

    This is useful for automation and debugging without MCP or HTTP.

    ### Running as an MCP server (uv)

    Start the server:

    ```cmd
    uv run sqlite-mcp
    ```

    Integration examples:

    - Claude Desktop configuration (add to `claude_desktop_config.json`):

    ```json
    "mcpServers": {
      "sqlite-mcp": {
        "command": "uv",
        "args": [
          "run",
          "--directory",
          "<path-to-sqlite-mcp-repo>",
          "sqlite-mcp"
        ]
      }
    }
    ```

    - VS Code / Cursor: add a `.vscode/mcp.json` in your workspace or paste the block into User Settings (JSON):

    ```json
    {
      "mcpServers": {
        "sqlite-mcp": {
          "command": "uv",
          "args": [
            "run",
            "--directory",
            "<path-to-sqlite-mcp-repo>",
            "sqlite-mcp"
          ]
        }
      }
    }
    ```

    You can also run directly from Git without cloning:

    ```cmd
    uv run --from https://github.com/rushikeshjaisur11/sqlite-mcp.git sqlite-mcp
    ```

    Note: adjust paths for Windows escaping when required.

    ### HTTP API (FastAPI + uvicorn)

    If you want remote/HTTP access, create a small `api.py` that imports the same service layer and exposes endpoints (example shown in the project issues). Then run:

    ```cmd
    python -m uvicorn api:app --host 127.0.0.1 --port 8000 --reload
    ```

    Example curl (Windows `cmd.exe`) usage — be careful with JSON quoting on cmd.exe:

    ```cmd
    curl -X POST "http://127.0.0.1:8000/set-db" -H "Content-Type: application/json" -d "{\"path\":\"C:\\path\\to\\my.db\"}"
    curl "http://127.0.0.1:8000/get-db"
    curl "http://127.0.0.1:8000/test-conn"
    ```

    ---

    ## Tools (MCP endpoints)

    The MCP server exposes a small collection of tools to interact with a SQLite database. All tools accept an optional `db_path` parameter; if omitted, the server uses the configured/default database path.

    - query_sqlite_table: Translate a natural language `user_text` into a `SELECT` query and run it.
    - list_all_tables: Return a list of all tables in the database.
    - get_table_preview: Return the first N rows from a table (preview).
    - get_column_statistics: Compute simple statistics for a column (count, distinct, nulls, min/max for numeric).
    - find_tables_by_column: Find tables that contain a given column name.
    - get_table_schema_info: Return the CREATE TABLE / column definitions for a table.
    - get_database_overview: Return a compact overview (tables, row counts, schemas).
    - test_connection: Validate that the configured `db_path` is reachable and can be opened.

    Refer to the inline docstrings in `utils/` for exact parameter names and return formats.

    ---

    ## Development

    - The project's core service code lives under `utils/` (for example `utils/sqllite_service.py`).
    - To add a CLI, API, or MCP router, import the service functions from `utils` and keep a single implementation for business logic.
    - Consider adding unit tests (pytest) for:
      - query translation
      - schema inspection
      - connection handling (happy path + failure)

    Suggested dev commands (Windows cmd):

    ```cmd
    python -m pip install -r requirements.txt    # if you maintain one
    python -m pytest -q                        # run tests (if added)
    ```

    ---

    ## Contributing

    Contributions are welcome. Please open issues or pull requests. Keep changes small and add tests when modifying core logic.

    ---

    ## License

    This project is released under the MIT License — see the `LICENSE` file for details.

    ---

    If you'd like, I can also:
    - Add a ready-to-run `cli.py` and `api.py` that import the existing `utils` service functions.
    - Update `pyproject.toml`/`requirements.txt` with optional dependencies (`fastapi`, `uvicorn`).

    Tell me which of the above you'd like me to add and I will create the files and run a quick smoke test.
