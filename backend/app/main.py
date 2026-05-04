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


@app.post("/api/chat")
async def chat(request: ChatRequest):
    if request.file_id is not None and get_record(request.file_id) is None:
        raise HTTPException(status_code=404, detail="File not found or expired")

    async def event_stream():
        async for item in stream_chat(request.messages, request.provider, request.file_id, request.api_key):
            if isinstance(item, ChartEvent):
                yield f"data: {json.dumps({'type': 'chart', 'content': item.figure})}\n\n"
            else:
                yield f"data: {json.dumps({'type': 'text', 'content': item})}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")
