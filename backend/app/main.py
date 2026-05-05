from dotenv import load_dotenv

load_dotenv()  # must run before agent imports so API keys are in env at model init time

import asyncio
import json
from contextlib import asynccontextmanager
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .agents.chat import stream_chat
from .agents.data_agent import ChartEvent
from .storage import cleanup_loop, get_record, rebuild_registry, store_file


@asynccontextmanager
async def lifespan(app: FastAPI):
    rebuild_registry()
    task = asyncio.create_task(cleanup_loop())
    yield
    task.cancel()


app = FastAPI(title="Material Agents API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["POST"],
    allow_headers=["Content-Type"],
)


# ── Upload ────────────────────────────────────────────────────────────────────

@app.post("/api/upload")
async def upload(file: UploadFile):
    data = await file.read()
    try:
        record = store_file(data, file.filename or "upload")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return {
        "file_id": record.file_id,
        "filename": record.filename,
        "rows": record.rows,
        "columns": record.columns,
    }


# ── Chat ──────────────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    messages: list[dict]
    provider: Literal["anthropic", "openai"] = "anthropic"
    file_id: Optional[str] = None
    api_key: Optional[str] = None  # user-supplied key; falls back to env-var key when absent


def _user_facing_error(exc: Exception) -> str:
    """Convert a provider exception into a short, actionable message for the UI."""
    msg = str(exc)
    msg_lower = msg.lower()
    exc_type = type(exc).__name__

    if (
        "401" in msg
        or "AuthenticationError" in exc_type
        or "authentication" in msg_lower
        or "incorrect api key" in msg_lower
        or "invalid api key" in msg_lower
        or "unauthorized" in msg_lower
    ):
        return "API key is invalid or incorrect. Open Options to check your key."

    if "429" in msg or "RateLimitError" in exc_type or "rate limit" in msg_lower:
        return "Rate limit reached. Please wait a moment and try again."

    if "insufficient_quota" in msg_lower or "quota" in msg_lower:
        return "API quota exceeded. Check your billing settings with the provider."

    return f"Request failed: {msg[:200]}"


@app.post("/api/chat")
async def chat(request: ChatRequest):
    if request.file_id is not None and get_record(request.file_id) is None:
        raise HTTPException(status_code=404, detail="File not found or expired")

    async def event_stream():
        try:
            async for item in stream_chat(request.messages, request.provider, request.file_id, request.api_key):
                if isinstance(item, ChartEvent):
                    yield f"data: {json.dumps({'type': 'chart', 'content': item.figure})}\n\n"
                else:
                    yield f"data: {json.dumps({'type': 'text', 'content': item})}\n\n"
        except Exception as exc:
            yield f"data: {json.dumps({'type': 'error', 'content': _user_facing_error(exc)})}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")
