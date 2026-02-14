import os
import requests
from typing import Dict, List, TypedDict


class ExecuteQueryResponse(TypedDict, total=False):
    rows: List[Dict[str, str]] | None
    columns: List[str] | None
    error: str | None

BASE_URL = os.getenv(
    "SCHEMA_API_URL",
    "http://localhost:8080"
)

def get_table_schema(table_name: str) -> str:
    url = f"{BASE_URL}/getschema/{table_name}"
    response = requests.get(url)

    response.raise_for_status()
    return response.json().get("schema")

def execute_query(query: str) -> ExecuteQueryResponse:
    url = f"{BASE_URL}/executequery"
    session = requests.Session()
    response = session.post(
        url,
        params={"query": query},
        timeout=10
    )

    response.raise_for_status()
    return response.json()

def extract_unique_nouns(table_name: str, noun_columns: List[str]) -> set:
    unique_nouns = set()

    column_names =  ', '.join(f'"{col}"' for col in noun_columns)
    query =  f'SELECT DISTINCT {column_names} FROM "{table_name}"'
    results = execute_query(query)
    
    if (results["rows"] is not None):
        for row in results["rows"]:
            unique_nouns.update(str(value) for value in row.values() if value)
    
    return unique_nouns