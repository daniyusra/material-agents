import json
from typing import List
import duckdb

DB_PATH = "/home/daniyusra/projects/manantara-playground/chatbot/db/data.duckdb" #TODO SHOULD BE CONFIGURABLE

#duck db static helpers
def csv_to_duckdb(csv_path: str,  table_name: str):
    """Converts csv_to_duckdb"""
    # Connect to (or create) DuckDB database
    conn = duckdb.connect(database=DB_PATH)

    # Use DuckDB's built-in CSV reader to infer schema and load data
    conn.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM read_csv_auto('{csv_path}', header=True)
    """)

        # Use DuckDB's built-in CSV reader to infer schema and load data
    conn.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM read_csv_auto('{csv_path}', header=True)
    """)

    conn.close()

def get_all_schema() -> str:
    """Gets schema of all tables"""
    # Connect to (or create) DuckDB database
    conn = duckdb.connect(database=DB_PATH)

    try:
        tables = conn.execute("SHOW TABLES;").fetchall()
        schema_lines = []
        for (table_name,) in tables:
            get_table_schema_str(conn, schema_lines, table_name)  # spacing between tables

        conn.close()

        schema_summary = "\n".join(schema_lines)
        return schema_summary
    except Exception as e:
        print(e)
        return ""

def get_table_schema(table_name : str) -> str:
    """Gets schema of tables"""
    # Connect to (or create) DuckDB database
    conn = duckdb.connect(database=DB_PATH)

    try:
        tables = conn.execute("SHOW TABLES;").fetchall()
        schema_lines = []
        get_table_schema_str(conn, schema_lines, table_name)

        conn.close()

        schema_summary = "\n".join(schema_lines)
        return schema_summary
    except Exception as e:
        print(e)
        return ""

def get_table_schema_str(conn, schema_lines, table_name):
    """Gets schema string of table"""
    schema_lines.append(f"TABLE NAME: {table_name.upper()}")
    schema_lines.append("")

            # 🧠 Get CREATE TABLE SQL
    schema_df = conn.execute(f"DESCRIBE {table_name};").fetchdf()
    schema_lines.append("Schema:")
    parts = []
    for _, row in schema_df.iterrows():
        cname = row["column_name"]
        ctype = row["column_type"]
        parts.append(f"  {cname} {ctype}")

    create_stmt = f"CREATE TABLE {table_name} (\n" + ",\n".join(parts) + "\n);"
    schema_lines.append(create_stmt)
            # Example rows
    example_rows = conn.execute(f"SELECT * FROM {table_name} LIMIT 3;").fetchall()
    if example_rows:
        columns = [col[0] for col in conn.execute(f"DESCRIBE {table_name};").fetchall()]
        schema_lines.append("")
        schema_lines.append("Example rows:")
        for row in example_rows:
            row_dict = dict(zip(columns, row))
            schema_lines.append("  " + json.dumps(row_dict, ensure_ascii=False))

    schema_lines.append("")

def extract_unique_nouns(table_name: str, noun_columns: List[str]) -> set:
    unique_nouns = set()

    conn = duckdb.connect(database=DB_PATH)
    column_names =  ', '.join(f'"{col}"' for col in noun_columns)
    query =  f'SELECT DISTINCT {column_names} FROM "{table_name}"'
    results = conn.execute(query).fetchall()
    conn.close()

    for row in results:
        unique_nouns.update(str(value) for value in row if value)
    
    return unique_nouns

def execute_sql_query(sql_query: str):
    """
    Execute a SQL query against a DuckDB database.

    Returns:
        - On success: {"ok": True, "rows": [...], "columns": [...]}
        - On failure: {"ok": False, "error": "message"}
    """
    try:
        # Connect with context manager so it auto-closes
        with duckdb.connect(database=DB_PATH) as conn:
            result = conn.execute(sql_query)
            
            # If query returns rows (SELECT), fetch them
            if result.description is not None:
                columns = [col[0] for col in result.description]
                rows = result.fetchall()
                
                return {
                    "ok": True,
                    "columns": columns,
                    "rows": [dict(zip(columns, row)) for row in rows]
                }
            else:
                # For INSERT/UPDATE/DDL
                return {
                    "ok": True,
                    "columns": [],
                    "rows": []
                }

    except duckdb.Error as e:
        return {"ok": False, "error": str(e)}

    except Exception as e:
        return {"ok": False, "error": f"Unexpected error: {str(e)}"}
    