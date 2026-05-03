"""
Executed as a subprocess by execute_pandas_code / execute_viz_code.
Reads a JSON payload from stdin, runs the code with restricted builtins,
writes a JSON result to stdout.

Payload schema:
  { "mode": "pandas" | "viz", "code": "...", "csv_path": "..." }

Result schema:
  { "result": "..." }          — pandas success
  { "figure": {...} }          — viz success
  { "error": "..." }           — any failure
"""
import builtins as _b
import json
import sys

import pandas as pd

_ALLOWED_BUILTINS = {
    "abs", "all", "any", "bool", "callable", "chr", "dict", "dir",
    "divmod", "enumerate", "filter", "float", "format", "frozenset",
    "getattr", "hasattr", "hash", "int", "isinstance", "issubclass",
    "iter", "len", "list", "map", "max", "min", "next", "object",
    "ord", "pow", "print", "range", "repr", "reversed", "round",
    "set", "slice", "sorted", "str", "sum", "tuple", "type", "zip",
    "True", "False", "None", "NotImplemented", "Ellipsis",
    "Exception", "ValueError", "TypeError", "KeyError", "IndexError",
    "AttributeError", "RuntimeError", "StopIteration",
}

def _safe_builtins() -> dict:
    safe = {name: getattr(_b, name) for name in _ALLOWED_BUILTINS if hasattr(_b, name)}
    safe.update({"True": True, "False": False, "None": None})
    return safe


def _run_pandas(code: str, df: pd.DataFrame) -> dict:
    g = {"__builtins__": _safe_builtins(), "pd": pd, "df": df}
    l: dict = {}
    try:
        exec(code, g, l)  # noqa: S102 — restricted builtins + AST-guarded before reaching here
    except Exception as e:
        return {"error": f"ExecutionError: {e}"}
    r = l.get("result")
    if r is None:
        return {"error": "(no `result` variable was assigned)"}
    return {"result": str(r)}


def _run_viz(code: str, df: pd.DataFrame) -> dict:
    import json as _json

    import plotly.express as px
    import plotly.graph_objects as go

    g = {"__builtins__": _safe_builtins(), "pd": pd, "px": px, "go": go, "df": df}
    l: dict = {}
    try:
        exec(code, g, l)  # noqa: S102
    except Exception as e:
        return {"error": f"ExecutionError: {e}"}
    r = l.get("result")
    if r is None:
        return {"error": "(no `result` variable was assigned)"}
    if not hasattr(r, "to_json"):
        return {"error": f"ExecutionError: `result` must be a Plotly Figure, got {type(r).__name__}"}
    return {"figure": _json.loads(r.to_json())}


if __name__ == "__main__":
    try:
        payload = json.loads(sys.stdin.buffer.read())
        df = pd.read_csv(payload["csv_path"])
        mode = payload.get("mode", "pandas")
        out = _run_pandas(payload["code"], df) if mode == "pandas" else _run_viz(payload["code"], df)
    except Exception as e:
        out = {"error": f"ExecutionError: {e}"}
    print(json.dumps(out))
