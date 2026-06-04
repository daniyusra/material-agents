"""
AI-powered extraction and generation for the WhatsApp bot.
Uses GPT-4o via LangChain, matching the existing provider pattern.
"""

import json
import logging
from datetime import datetime, timezone

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

log = logging.getLogger(__name__)

# Non-streaming singleton — we never need incremental output here.
_model = ChatOpenAI(model="gpt-4o", streaming=False)


def _format_messages(messages: list[dict]) -> str:
    lines = []
    for m in messages:
        name = m.get("sender_name") or m.get("sender_jid", "Unknown")
        ts = m.get("ts")
        ts_str = (
            f" [{datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}]"
            if ts else ""
        )
        lines.append(f"{name}{ts_str}: {m['content']}")
    return "\n".join(lines)


async def extract_todos(messages: list[dict]) -> list[dict]:
    """Extract action items from messages. Returns list of {text, owner}."""
    if not messages:
        return []
    conversation = _format_messages(messages)
    prompt = (
        "Extract action items from this WhatsApp group conversation.\n"
        'Return a JSON array of objects with "text" (the task) and "owner" (person responsible, or null).\n'
        "Only include concrete, actionable items. If none exist, return [].\n\n"
        f"Conversation:\n{conversation}\n\n"
        "Return ONLY valid JSON, no explanation:"
    )
    try:
        resp = await _model.ainvoke([HumanMessage(content=prompt)])
        text = resp.content.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        result = json.loads(text)
        return result if isinstance(result, list) else []
    except Exception as exc:
        log.error("extract_todos_error", extra={"error": str(exc)})
        return []


async def generate_recap(messages: list[dict]) -> str:
    """Generate a concise recap of recent group messages."""
    if not messages:
        return "No recent messages to summarize."
    conversation = _format_messages(messages[-100:])
    system = SystemMessage(
        content=(
            "You are a helpful group assistant. Summarize the conversation into a concise recap. "
            "Cover key decisions, action items, important updates, and open questions. "
            "Use bullet points. Keep it under 300 words."
        )
    )
    user = HumanMessage(content=f"Conversation:\n{conversation}")
    try:
        resp = await _model.ainvoke([system, user])
        return resp.content.strip()
    except Exception as exc:
        log.error("generate_recap_error", extra={"error": str(exc)})
        return "Could not generate recap at this time."


async def answer_question(question: str, messages: list[dict]) -> str:
    """Answer a question using recent group message context."""
    context = _format_messages(messages[-50:])
    system = SystemMessage(
        content="You are a helpful assistant for a WhatsApp group. Answer questions using the provided conversation context concisely."
    )
    user = HumanMessage(content=f"Recent conversation:\n{context}\n\nQuestion: {question}")
    try:
        resp = await _model.ainvoke([system, user])
        return resp.content.strip()
    except Exception as exc:
        log.error("answer_question_error", extra={"error": str(exc)})
        return "Could not process that request."
