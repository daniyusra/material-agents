from dotenv import load_dotenv

load_dotenv()  # must run before agent imports so API keys are in env at model init time

import json
from typing import Literal

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .agents.chat import stream_chat as stream_anthropic
from .agents.chat_openai import stream_chat as stream_openai

app = FastAPI(title="Material Agents API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["POST"],
    allow_headers=["Content-Type"],
)

_providers = {
    "anthropic": stream_anthropic,
    "openai": stream_openai,
}


class ChatRequest(BaseModel):
    messages: list[dict]
    provider: Literal["anthropic", "openai"] = "anthropic"


@app.post("/api/chat")
async def chat(request: ChatRequest):
    stream_fn = _providers[request.provider]

    async def event_stream():
        async for chunk in stream_fn(request.messages):
            yield f"data: {json.dumps({'content': chunk})}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")
