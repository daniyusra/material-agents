"""
Unified chat module. Selects the LLM provider and routes requests to the
data agent when a file_id is present.
"""

from typing import AsyncIterator, Literal

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

# Module-level singletons — used as fallback when the caller does not supply a key.
# load_dotenv() in main.py runs before this module is imported.
_anthropic = ChatAnthropic(model="claude-opus-4-7", streaming=True, max_tokens=4096)
_openai = ChatOpenAI(model="gpt-4o", streaming=True)


def get_model(provider: Literal["anthropic", "openai"], api_key: str | None = None):
    """Return a model instance for *provider*.

    If *api_key* is provided, a fresh instance is created using that key.
    Otherwise the module-level singleton (env-var key) is returned as fallback.
    """
    if provider == "anthropic":
        if api_key:
            return ChatAnthropic(model="claude-opus-4-7", streaming=True, max_tokens=4096, api_key=api_key)
        return _anthropic
    if provider == "openai":
        if api_key:
            return ChatOpenAI(model="gpt-4o", streaming=True, api_key=api_key)
        return _openai
    raise ValueError(f"Unknown provider: {provider!r}")


def _to_lc_messages(messages: list[dict]) -> list:
    result = []
    for msg in messages:
        role = msg.get("role")
        content = msg.get("content", "")
        if role == "user":
            result.append(HumanMessage(content=content))
        elif role == "assistant":
            result.append(AIMessage(content=content))
        elif role == "system":
            result.append(SystemMessage(content=content))
    return result


async def stream_chat(
    messages: list[dict],
    provider: Literal["anthropic", "openai"] = "anthropic",
    file_id: str | None = None,
    api_key: str | None = None,
) -> AsyncIterator:
    if file_id is not None:
        from .data_agent import stream_data_chat
        async for item in stream_data_chat(messages, provider, file_id, api_key):
            yield item
        return

    model = get_model(provider, api_key)
    async for chunk in model.astream(_to_lc_messages(messages)):
        if chunk.content:
            yield chunk.content
