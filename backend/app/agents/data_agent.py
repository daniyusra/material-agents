"""
Data Q&A + Visualization agent implemented as a LangGraph StateGraph.

Graph topology:
  START → classify → route_by_intent
      NOT_DATA               → plain_chat  → END
      DATA_ONLY / DATA_VIZ   → generate_code → execute_code → route_after_execute
                                                   → fix_code → execute_code  (retry once)
                                                   → synthesize → END

For DATA_VIZ, generate_code emits Plotly code; execute_code sets plotly_json in state.
stream_data_chat yields str tokens (from synthesize / plain_chat) and then a single
ChartEvent at the very end when a figure was produced.
"""

import ast
import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Literal, TypedDict

import pandas as pd
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph

from ..storage import get_dataframe, get_record
from .chat import _to_lc_messages, get_model

# ── Chart event ───────────────────────────────────────────────────────────────

@dataclass
class ChartEvent:
    """Yielded by stream_data_chat when a Plotly figure was produced."""
    figure: dict


# ── Prompts ───────────────────────────────────────────────────────────────────

_CLASSIFY_SYSTEM = """\
You are a data analyst assistant. The user has uploaded a dataset.
Dataset columns: {columns}

Classify the user's question into one of three intents and respond with ONLY valid JSON.

Intents:
- NOT_DATA: Not about the dataset (greetings, general knowledge, etc.)
- DATA_ONLY: Requires data analysis but NO chart (statistics, specific values)
- DATA_VIZ: Requires a chart visualization

If DATA_VIZ, also choose the most appropriate chart type:
- histogram    — distribution of a single variable ("Show me the distribution of X")
- boxplot      — compare distributions across groups ("Compare A vs B vs C")
- scatter      — relationship / correlation between two variables ("Is X correlated with Y?")
- heatmap      — correlation matrix across all numeric variables ("Which variables matter most?")
- line         — trend over a sequence or time axis ("How does X change across experiments?")
- scatter_matrix — overview of all pairwise variable relationships ("Show me everything at once")

Respond with ONLY valid JSON, no explanation:
{{"intent": "NOT_DATA"|"DATA_ONLY"|"DATA_VIZ", "chart_type": null|"histogram"|"boxplot"|"scatter"|"heatmap"|"line"|"scatter_matrix"}}"""

_DATA_CODE_SYSTEM = """\
You have access to a pandas DataFrame called `df`.

{df_info}

Write Python/pandas code to answer the user's question.
Rules:
- `df` and `pd` are in scope — do not import anything.
- Store the final answer in a variable named `result`.
- Output ONLY raw Python code. No markdown fences, no explanation."""

_VIZ_CODE_SYSTEM = """\
You have access to a pandas DataFrame called `df`.

{df_info}

Create a {chart_type} chart to answer the user's question.
In scope: `df`, `pd`, `px` (plotly.express), `go` (plotly.graph_objects).
Assign the final Plotly Figure to a variable named `result`.
Output ONLY raw Python code. No markdown fences, no explanation.

Chart type guidelines:
- histogram:      result = px.histogram(df, x="col")
- boxplot:        result = px.box(df, x="group_col", y="value_col")
- scatter:        result = px.scatter(df, x="col1", y="col2", trendline="ols")
- heatmap:        result = px.imshow(df.select_dtypes("number").corr(), text_auto=True, color_continuous_scale="RdBu_r", zmin=-1, zmax=1)
- line:           result = px.line(df, x="x_col", y="y_col")
- scatter_matrix: result = px.scatter_matrix(df.select_dtypes("number"))"""

_FIX_CODE_SYSTEM = """\
The following Python code raised an error when executed. Rewrite it so it works correctly.
Output ONLY the corrected Python code. No markdown fences, no explanation.

Original code:
{code}

Error:
{error}"""

_SYNTHESIS_SYSTEM = """\
You are a data analyst answering a user's question with precision.
Lead with the direct answer, then cite the exact figure from the code result.
Add 1–2 sentences of interpretation or context if it adds value (comparisons, what the number implies, caveats).
Keep the tone conversational, not academic. If the result is an error, apologise briefly and suggest what went wrong.
Dataset: {filename} — columns: {columns}"""

_SYNTHESIS_VIZ_SYSTEM = """\
You are a data analyst answering a user's question directly. A {chart_type} was just generated.
Lead with a clear conclusion, then back it up with specific observations — reference actual values, ranges, \
or trends visible from the dataset info and the chart code (e.g. which columns were plotted, what the sample \
rows suggest about the distribution, whether a trendline was used).
Do not describe chart mechanics. Do not say "the chart shows" — instead say what the data shows.
Keep the tone conversational. 3–5 sentences is fine if the question warrants it.
Dataset: {filename} — columns: {columns}"""


# ── State ─────────────────────────────────────────────────────────────────────

class DataAgentState(TypedDict):
    messages: list[dict]
    provider: str
    file_id: str
    api_key: str | None         # user-supplied key; falls back to env-var singleton when None
    question: str
    intent: str | None          # NOT_DATA | DATA_ONLY | DATA_VIZ
    chart_type: str | None      # histogram | boxplot | scatter | heatmap | line | scatter_matrix
    generated_code: str | None
    execution_result: str | None
    plotly_json: dict | None
    retry_count: int


# ── Pure helpers (independently testable) ─────────────────────────────────────

_BLOCKED_ATTRS = frozenset({
    "system", "popen", "exec", "eval", "compile",
    "open", "read", "write", "remove", "unlink", "rmdir",
})

_RUNNER = Path(__file__).parent / "_sandbox_runner.py"

# Env passed to the sandbox subprocess — strips API keys and credentials while
# keeping what Python needs to locate its stdlib and any virtualenv.
_SAFE_ENV_KEYS = {"PATH", "PYTHONPATH", "VIRTUAL_ENV", "PYTHONHOME", "LANG", "LC_ALL", "HOME"}
_SANDBOX_ENV = {k: v for k, v in os.environ.items() if k in _SAFE_ENV_KEYS}


def _ast_guard(code: str) -> str | None:
    """Level 2: Return an error string if the code contains blocked patterns, else None."""
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        return f"SyntaxError: {e}"
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            return "import statements are not allowed in generated code"
        if isinstance(node, ast.Attribute):
            if node.attr.startswith("__") or node.attr in _BLOCKED_ATTRS:
                return f"blocked attribute access: .{node.attr}"
    return None


def _run_sandboxed(mode: str, code: str, df: pd.DataFrame, timeout: int = 15) -> dict:
    """Level 3: serialise df to a temp CSV, execute code in a subprocess, return parsed result."""
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        csv_path = f.name
    try:
        df.to_csv(csv_path, index=False)
        payload = json.dumps({"mode": mode, "code": code, "csv_path": csv_path}).encode()
        proc = subprocess.run(
            [sys.executable, str(_RUNNER)],
            input=payload,
            capture_output=True,
            timeout=timeout,
            env=_SANDBOX_ENV,
        )
    except subprocess.TimeoutExpired:
        return {"error": "ExecutionError: execution timed out (15 s limit)"}
    finally:
        Path(csv_path).unlink(missing_ok=True)

    if proc.returncode != 0:
        stderr = proc.stderr.decode(errors="replace").strip()
        return {"error": f"ExecutionError: {stderr[:300] or 'subprocess failed'}"}

    stdout = proc.stdout.decode(errors="replace").strip()
    try:
        return json.loads(stdout.split("\n")[-1])
    except (json.JSONDecodeError, IndexError):
        return {"error": "ExecutionError: unexpected subprocess output"}


def df_info(df: pd.DataFrame) -> str:
    """Return a compact schema + sample string for LLM prompts.

    String values are truncated to 120 chars to limit prompt-injection surface
    (a cell containing 'Ignore previous instructions…' would otherwise reach the LLM verbatim).
    """
    sample = df.head(3).copy()
    for col in sample.select_dtypes(include=["object", "str"]).columns:
        sample[col] = sample[col].astype(str).str[:120]
    lines = [
        f"Shape: {df.shape[0]} rows × {df.shape[1]} columns",
        "Columns (name: dtype): "
        + ", ".join(f"{c}: {t}" for c, t in zip(df.columns, df.dtypes)),
        "",
        "Sample (first 3 rows):",
        sample.to_string(index=False),
    ]
    return "\n".join(lines)


def df_stats(df: pd.DataFrame) -> str:
    """Return descriptive statistics for numeric columns — gives synthesis node actual values to cite."""
    numeric = df.select_dtypes(include="number")
    if numeric.empty:
        return "No numeric columns."
    return numeric.describe().round(4).to_string()


def execute_pandas_code(code: str, df: pd.DataFrame) -> str:
    """Execute pandas code in a sandboxed subprocess. Returns str(result) or 'ExecutionError: …'."""
    if err := _ast_guard(code):
        return f"ExecutionError: {err}"
    result = _run_sandboxed("pandas", code, df)
    return result.get("error") or result.get("result", "(no `result` variable was assigned)")


def execute_viz_code(code: str, df: pd.DataFrame) -> tuple[str, dict | None]:
    """Execute Plotly viz code in a sandboxed subprocess. Returns (status, figure_dict | None)."""
    if err := _ast_guard(code):
        return f"ExecutionError: {err}", None
    result = _run_sandboxed("viz", code, df)
    if "error" in result:
        err_msg = result["error"]
        return err_msg, None
    return "Chart generated successfully.", result.get("figure")


def _strip_fences(text: str) -> str:
    """Remove optional ``` fences the model may add despite instructions."""
    text = re.sub(r"^```(?:python|json)?\s*\n?", "", text.strip(), flags=re.IGNORECASE)
    return re.sub(r"\n?```\s*$", "", text)


def _parse_classification(text: str) -> tuple[str, str | None]:
    """Parse classify node JSON response. Falls back to DATA_ONLY on any error."""
    try:
        data = json.loads(_strip_fences(text))
        intent = data.get("intent", "DATA_ONLY")
        if intent not in {"NOT_DATA", "DATA_ONLY", "DATA_VIZ"}:
            intent = "DATA_ONLY"
        return intent, data.get("chart_type")
    except Exception:
        return "DATA_ONLY", None


# ── Graph nodes ───────────────────────────────────────────────────────────────

async def _classify_node(state: DataAgentState) -> dict:
    record = get_record(state["file_id"])
    model = get_model(state["provider"], state.get("api_key"))
    columns = ", ".join(record.columns) if record else "unknown"
    prompt = [
        SystemMessage(content=_CLASSIFY_SYSTEM.format(columns=columns)),
        HumanMessage(content=state["question"]),
    ]
    response = await model.ainvoke(prompt)
    intent, chart_type = _parse_classification(str(response.content))
    return {"intent": intent, "chart_type": chart_type}


async def _generate_code_node(state: DataAgentState) -> dict:
    df = get_dataframe(state["file_id"])
    model = get_model(state["provider"], state.get("api_key"))
    system = (
        _VIZ_CODE_SYSTEM.format(
            df_info=df_info(df),
            chart_type=state.get("chart_type") or "chart",
        )
        if state["intent"] == "DATA_VIZ"
        else _DATA_CODE_SYSTEM.format(df_info=df_info(df))
    )
    response = await model.ainvoke([
        SystemMessage(content=system),
        HumanMessage(content=state["question"]),
    ])
    return {"generated_code": _strip_fences(str(response.content))}


async def _execute_code_node(state: DataAgentState) -> dict:
    df = get_dataframe(state["file_id"])
    code = state["generated_code"] or ""
    if state["intent"] == "DATA_VIZ":
        status, figure = execute_viz_code(code, df)
        return {"execution_result": status, "plotly_json": figure}
    return {"execution_result": execute_pandas_code(code, df), "plotly_json": None}


async def _fix_code_node(state: DataAgentState) -> dict:
    model = get_model(state["provider"], state.get("api_key"))
    response = await model.ainvoke([
        SystemMessage(content=_FIX_CODE_SYSTEM.format(
            code=state["generated_code"],
            error=state["execution_result"],
        )),
    ])
    return {
        "generated_code": _strip_fences(str(response.content)),
        "retry_count": state["retry_count"] + 1,
    }


async def _synthesize_node(state: DataAgentState) -> dict:
    record = get_record(state["file_id"])
    model = get_model(state["provider"], state.get("api_key"))
    columns = ", ".join(record.columns) if record else "unknown"
    filename = record.filename if record else "dataset"

    if state["intent"] == "DATA_VIZ":
        system = _SYNTHESIS_VIZ_SYSTEM.format(
            chart_type=state.get("chart_type") or "chart",
            filename=filename,
            columns=columns,
        )
        df = get_dataframe(state["file_id"])
        data_context = df_info(df) if df is not None else ""
        stats_context = df_stats(df) if df is not None else ""
        user_msg = (
            f"Question: {state['question']}\n\n"
            f"Dataset info:\n{data_context}\n\n"
            f"Descriptive statistics:\n{stats_context}\n\n"
            f"Chart code executed:\n{state.get('generated_code') or ''}"
        )
    else:
        system = _SYNTHESIS_SYSTEM.format(filename=filename, columns=columns)
        user_msg = f"Question: {state['question']}\n\nCode result:\n{state['execution_result']}"

    await model.ainvoke([SystemMessage(content=system), HumanMessage(content=user_msg)])
    return {}


async def _plain_chat_node(state: DataAgentState) -> dict:
    model = get_model(state["provider"], state.get("api_key"))
    await model.ainvoke(_to_lc_messages(state["messages"]))
    return {}


# ── Routing ───────────────────────────────────────────────────────────────────

def route_by_intent(state: DataAgentState) -> str:
    return "plain_chat" if state["intent"] == "NOT_DATA" else "generate_code"


def route_after_execute(state: DataAgentState) -> str:
    result = state.get("execution_result", "")
    if result.startswith("ExecutionError:") and state["retry_count"] < 1:
        return "fix_code"
    return "synthesize"


# ── Graph compilation (lazy singleton) ────────────────────────────────────────

_graph: CompiledStateGraph | None = None


def _build_graph() -> CompiledStateGraph:
    g = StateGraph(DataAgentState)

    g.add_node("classify", _classify_node)
    g.add_node("generate_code", _generate_code_node)
    g.add_node("execute_code", _execute_code_node)
    g.add_node("fix_code", _fix_code_node)
    g.add_node("synthesize", _synthesize_node)
    g.add_node("plain_chat", _plain_chat_node)

    g.add_edge(START, "classify")
    g.add_conditional_edges("classify", route_by_intent)
    g.add_edge("generate_code", "execute_code")
    g.add_conditional_edges("execute_code", route_after_execute)
    g.add_edge("fix_code", "execute_code")
    g.add_edge("synthesize", END)
    g.add_edge("plain_chat", END)

    return g.compile()


def _get_graph() -> CompiledStateGraph:
    global _graph
    if _graph is None:
        _graph = _build_graph()
    return _graph


# ── Public entry point ────────────────────────────────────────────────────────

_STREAMING_NODES = {"synthesize", "plain_chat"}


async def stream_data_chat(
    messages: list[dict],
    provider: Literal["anthropic", "openai"],
    file_id: str,
    api_key: str | None = None,
) -> AsyncIterator[str | ChartEvent]:
    if get_record(file_id) is None:
        yield "The uploaded file could not be found or has expired. Please re-upload."
        return

    if not messages:
        return

    initial_state: DataAgentState = {
        "messages": messages,
        "provider": provider,
        "file_id": file_id,
        "api_key": api_key,
        "question": messages[-1].get("content", ""),
        "intent": None,
        "chart_type": None,
        "generated_code": None,
        "execution_result": None,
        "plotly_json": None,
        "retry_count": 0,
    }

    plotly_json: dict | None = None

    async for event in _get_graph().astream_events(initial_state, version="v2"):
        node = event.get("metadata", {}).get("langgraph_node")

        # Yield text tokens from human-facing nodes
        if event["event"] == "on_chat_model_stream" and node in _STREAMING_NODES:
            chunk = event["data"]["chunk"]
            if chunk.content:
                yield chunk.content

        # Capture chart JSON from the last execute_code completion
        if event["event"] == "on_chain_end" and node == "execute_code":
            output = event["data"].get("output", {})
            if isinstance(output, dict) and output.get("plotly_json"):
                plotly_json = output["plotly_json"]

    # Emit the chart after all text is streamed
    if plotly_json is not None:
        yield ChartEvent(figure=plotly_json)
