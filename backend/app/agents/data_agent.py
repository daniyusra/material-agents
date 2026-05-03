"""
Data Q&A agent implemented as a LangGraph StateGraph.

Graph topology:
  START
    └─ generate_code
          ├─ (NOT_DATA_QUESTION) ─→ plain_chat ─→ END
          └─ (data question)    ─→ execute_code
                                        ├─ (error, retry < 1) ─→ fix_code ─→ execute_code
                                        └─ (success / retries exhausted) ─→ synthesize ─→ END

Streaming: astream_events filters on_chat_model_stream from STREAMING_NODES so
           only the final human-facing response is yielded token-by-token.
"""

import re
from typing import AsyncIterator, Literal, TypedDict

import pandas as pd
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph

from ..storage import get_dataframe, get_record
from .chat import _to_lc_messages, get_model

# ── Prompts ───────────────────────────────────────────────────────────────────

_CODE_SYSTEM = """\
You have access to a pandas DataFrame called `df`.

{df_info}

If the user's question is about analysing or querying this data, write Python code to answer it.
Rules:
- `df` and `pd` are already in scope — do not import anything.
- Store the final answer in a variable named `result`.
- Output ONLY the raw Python code. No markdown fences, no explanation.

If the question is NOT about the data (e.g. greetings, general knowledge), respond with exactly:
NOT_DATA_QUESTION"""

_FIX_SYSTEM = """\
The following Python/pandas code raised an error when executed against a DataFrame.
Rewrite it so it works correctly.
Output ONLY the corrected Python code. No markdown fences, no explanation.

Original code:
{code}

Error:
{error}"""

_SYNTHESIS_SYSTEM = """\
You are a helpful data analyst. The user asked a question about their dataset.
Pandas code was run and produced a result. Explain the result clearly and concisely.
If the result looks like an error, apologise briefly and suggest what might have gone wrong.
Dataset columns: {columns}"""

# ── State ─────────────────────────────────────────────────────────────────────

class DataAgentState(TypedDict):
    messages: list[dict]
    provider: str
    file_id: str
    question: str
    generated_code: str | None
    execution_result: str | None
    retry_count: int
    is_data_question: bool

# ── Pure helpers (independently testable) ─────────────────────────────────────

def df_info(df: pd.DataFrame) -> str:
    """Return a compact schema + sample string for use in LLM prompts."""
    lines = [
        f"Shape: {df.shape[0]} rows × {df.shape[1]} columns",
        "Columns (name: dtype): "
        + ", ".join(f"{c}: {t}" for c, t in zip(df.columns, df.dtypes)),
        "",
        "Sample (first 3 rows):",
        df.head(3).to_string(index=False),
    ]
    return "\n".join(lines)


def execute_pandas_code(code: str, df: pd.DataFrame) -> str:
    """
    Execute LLM-generated pandas code with *df* and *pd* in scope.
    Returns str(result) on success, or 'ExecutionError: …' on failure.
    """
    local_vars: dict = {"df": df, "pd": pd}
    try:
        exec(code, local_vars)  # nosec — personal research tool, single user
        result = local_vars.get("result", "(no `result` variable was assigned)")
        return str(result)
    except Exception as exc:
        return f"ExecutionError: {exc}"


def _strip_fences(text: str) -> str:
    """Remove optional ```python … ``` fences the model may add despite instructions."""
    text = re.sub(r"^```(?:python)?\s*\n?", "", text.strip(), flags=re.IGNORECASE)
    return re.sub(r"\n?```\s*$", "", text)

# ── Graph nodes ───────────────────────────────────────────────────────────────

async def _generate_code_node(state: DataAgentState) -> dict:
    model = get_model(state["provider"])
    data = get_dataframe(state["file_id"])
    prompt = [
        SystemMessage(content=_CODE_SYSTEM.format(df_info=df_info(data))),
        HumanMessage(content=state["question"]),
    ]
    response = await model.ainvoke(prompt)
    raw = _strip_fences(str(response.content))
    return {
        "generated_code": raw,
        "is_data_question": not raw.strip().startswith("NOT_DATA_QUESTION"),
    }


async def _execute_code_node(state: DataAgentState) -> dict:
    df = get_dataframe(state["file_id"])
    result = execute_pandas_code(state["generated_code"] or "", df)
    return {"execution_result": result}


async def _fix_code_node(state: DataAgentState) -> dict:
    model = get_model(state["provider"])
    prompt = [
        SystemMessage(content=_FIX_SYSTEM.format(
            code=state["generated_code"],
            error=state["execution_result"],
        )),
    ]
    response = await model.ainvoke(prompt)
    return {
        "generated_code": _strip_fences(str(response.content)),
        "retry_count": state["retry_count"] + 1,
    }


async def _synthesize_node(state: DataAgentState) -> dict:
    record = get_record(state["file_id"])
    model = get_model(state["provider"])
    prompt = [
        SystemMessage(content=_SYNTHESIS_SYSTEM.format(
            columns=", ".join(record.columns) if record else ""
        )),
        HumanMessage(content=f"Question: {state['question']}\n\nCode result:\n{state['execution_result']}"),
    ]
    await model.ainvoke(prompt)
    return {}


async def _plain_chat_node(state: DataAgentState) -> dict:
    model = get_model(state["provider"])
    await model.ainvoke(_to_lc_messages(state["messages"]))
    return {}

# ── Routing ───────────────────────────────────────────────────────────────────

def route_after_code(state: DataAgentState) -> str:
    return "execute_code" if state["is_data_question"] else "plain_chat"


def route_after_execute(state: DataAgentState) -> str:
    result = state.get("execution_result", "")
    if result.startswith("ExecutionError:") and state["retry_count"] < 1:
        return "fix_code"
    return "synthesize"

# ── Graph compilation (lazy singleton) ───────────────────────────────────────

_graph: CompiledStateGraph | None = None


def _build_graph() -> CompiledStateGraph:
    g = StateGraph(DataAgentState)

    g.add_node("generate_code", _generate_code_node)
    g.add_node("execute_code", _execute_code_node)
    g.add_node("fix_code", _fix_code_node)
    g.add_node("synthesize", _synthesize_node)
    g.add_node("plain_chat", _plain_chat_node)

    g.add_edge(START, "generate_code")
    g.add_conditional_edges("generate_code", route_after_code)
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
) -> AsyncIterator[str]:
    if get_record(file_id) is None:
        yield "The uploaded file could not be found or has expired. Please re-upload."
        return

    if not messages:
        return

    initial_state: DataAgentState = {
        "messages": messages,
        "provider": provider,
        "file_id": file_id,
        "question": messages[-1].get("content", ""),
        "generated_code": None,
        "execution_result": None,
        "retry_count": 0,
        "is_data_question": False,
    }

    async for event in _get_graph().astream_events(initial_state, version="v2"):
        if (
            event["event"] == "on_chat_model_stream"
            and event.get("metadata", {}).get("langgraph_node") in _STREAMING_NODES
        ):
            chunk = event["data"]["chunk"]
            if chunk.content:
                yield chunk.content
