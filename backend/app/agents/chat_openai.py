from typing import AsyncIterator

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

model = ChatOpenAI(
    model="gpt-4o",
    streaming=True,
)


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


async def stream_chat(messages: list[dict]) -> AsyncIterator[str]:
    lc_messages = _to_lc_messages(messages)
    async for chunk in model.astream(lc_messages):
        if chunk.content:
            yield chunk.content
