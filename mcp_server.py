import sqlite3
import duckdb
import sys
import os
import io
import contextlib
from fastmcp import FastMCP

# Initialize FastMCP Server
mcp = FastMCP("Agency-Unified-Server")

# Keep a global state dictionary for python execution (persistent REPL)
_REPL_GLOBALS = {}

# ─────────────────────────────────────────────────────────────────────────────
# 🐍 Python Tools
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def execute_python_code(code: str) -> str:
    """
    Executes arbitrary Python code in a persistent state session.
    Captures stdout and stderr, returning any printed output or errors.
    """
    global _REPL_GLOBALS
    stdout = io.StringIO()
    stderr = io.StringIO()
    
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        try:
            # Try to evaluate as an expression first (to return value directly)
            try:
                compiled = compile(code.strip(), "<mcp-repl>", "eval")
                result = eval(compiled, _REPL_GLOBALS)
                if result is not None:
                    print(result)
            except SyntaxError:
                # If evaluation fails, execute as statements
                compiled = compile(code, "<mcp-repl>", "exec")
                exec(compiled, _REPL_GLOBALS)
        except Exception as e:
            import traceback
            traceback.print_exc(file=sys.stderr)
            
    output = stdout.getvalue()
    errors = stderr.getvalue()
    
    response = []
    if output:
        response.append(output)
    if errors:
        response.append(f"Errors/Traceback:\n{errors}")
        
    return "\n".join(response) if response else "Code executed successfully (no output)."


# ─────────────────────────────────────────────────────────────────────────────
# 💾 SQL / Database Tools
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def query_sqlite(db_path: str, sql: str) -> str:
    """
    Executes a SQL query on a local SQLite database and returns the rows.
    Example db_path: "C:\\path\\to\\my_database.db"
    """
    if not os.path.exists(db_path):
        return f"Error: SQLite database file not found at {db_path}"
        
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(sql)
        
        # If it's a DDL or modification query (INSERT/UPDATE/CREATE)
        if cursor.description is None:
            conn.commit()
            rows_affected = conn.changes()
            conn.close()
            return f"Query executed successfully. Rows affected: {rows_affected}"
            
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            return "Query completed successfully. Empty result set."
            
        # Format table output
        header = " | ".join(columns)
        divider = "-" * len(header)
        data_rows = [" | ".join(str(val) for val in row) for row in rows]
        return "\n".join([header, divider] + data_rows)
        
    except Exception as e:
        return f"Error executing SQLite query: {str(e)}"


@mcp.tool()
def query_duckdb(db_path: str, sql: str) -> str:
    """
    Executes a SQL query on a local DuckDB database file (e.g. C:\\DuckDB\\my_db.duckdb)
    and returns the result. Supports read/write.
    """
    # Ensure parent directory exists
    dir_name = os.path.dirname(db_path)
    if dir_name and not os.path.exists(dir_name):
        return f"Error: Database directory {dir_name} does not exist."
        
    try:
        conn = duckdb.connect(db_path)
        df = conn.execute(sql).df()
        conn.close()
        
        if df.empty:
            return "Query completed successfully. Empty result set."
            
        return df.to_string(index=False)
    except Exception as e:
        return f"Error executing DuckDB query: {str(e)}"

if __name__ == "__main__":
    mcp.run()
