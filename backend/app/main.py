from dotenv import load_dotenv

load_dotenv()  # must run before agent imports so API keys are in env at model init time

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, field_validator
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from .agents.chat import stream_chat
from .agents.data_agent import ChartEvent
from .logging_config import configure_logging, configure_sentry
from .storage import cleanup_loop, get_record, rebuild_registry, store_file
from .whatsapp import db as wa_db
from .whatsapp.router import router as whatsapp_router
from .whatsapp.scheduler import setup_scheduler

# ── Observability ─────────────────────────────────────────────────────────────

configure_logging()
configure_sentry()

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

_WHATSAPP_ENABLED = os.getenv("WHATSAPP_ENABLED", "false").lower() == "true"
_ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "http://localhost:5173").split(",")]
_MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "10"))
_MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "50"))
_MAX_MESSAGE_LENGTH = int(os.getenv("MAX_MESSAGE_LENGTH", "8000"))
_UPLOAD_RATE = os.getenv("RATELIMIT_UPLOAD", "10/minute")
_CHAT_RATE = os.getenv("RATELIMIT_CHAT", "30/minute")

# ── Rate limiter ──────────────────────────────────────────────────────────────

_limiter_enabled = os.getenv("RATELIMIT_ENABLED", "true").lower() == "true"
limiter = Limiter(key_func=get_remote_address, enabled=_limiter_enabled)

# ── App ───────────────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    count = rebuild_registry()
    if _WHATSAPP_ENABLED:
        wa_db.init_db()
    log.info("startup_complete", extra={"restored_files": count, "whatsapp_enabled": _WHATSAPP_ENABLED})
    task = asyncio.create_task(cleanup_loop())
    _scheduler = None
    if _WHATSAPP_ENABLED:
        _scheduler = setup_scheduler()
        _scheduler.start()
    yield
    task.cancel()
    if _scheduler:
        _scheduler.shutdown(wait=False)
    log.info("shutdown_complete")


app = FastAPI(title="Material Agents API", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)

if _WHATSAPP_ENABLED:
    app.include_router(whatsapp_router)


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


# ── Upload ────────────────────────────────────────────────────────────────────

@app.post("/api/upload")
@limiter.limit(_UPLOAD_RATE)
async def upload(request: Request, file: UploadFile):
    data = await file.read()
    if len(data) > _MAX_UPLOAD_MB * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"File too large (max {_MAX_UPLOAD_MB} MB)")
    try:
        record = store_file(data, file.filename or "upload")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    log.info("file_uploaded", extra={
        "file_id": record.file_id,
        "upload_filename": record.filename,
        "rows": record.rows,
        "columns": len(record.columns),
    })
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

    @field_validator("messages")
    @classmethod
    def validate_messages(cls, v: list[dict]) -> list[dict]:
        if len(v) > _MAX_MESSAGES:
            raise ValueError(f"Too many messages (max {_MAX_MESSAGES})")
        for msg in v:
            content = msg.get("content", "")
            if len(content) > _MAX_MESSAGE_LENGTH:
                raise ValueError(f"Message exceeds max length ({_MAX_MESSAGE_LENGTH} chars)")
        return v


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
@limiter.limit(_CHAT_RATE)
async def chat(request: Request, body: ChatRequest):
    if body.file_id is not None and get_record(body.file_id) is None:
        raise HTTPException(status_code=404, detail="File not found or expired")

    start = time.monotonic()

    async def event_stream():
        try:
            async for item in stream_chat(body.messages, body.provider, body.file_id, body.api_key):
                if isinstance(item, ChartEvent):
                    yield f"data: {json.dumps({'type': 'chart', 'content': item.figure})}\n\n"
                else:
                    yield f"data: {json.dumps({'type': 'text', 'content': item})}\n\n"
        except Exception as exc:
            log.error("chat_error", extra={"provider": body.provider, "error": str(exc)}, exc_info=True)
            yield f"data: {json.dumps({'type': 'error', 'content': _user_facing_error(exc)})}\n\n"
        finally:
            duration_ms = round((time.monotonic() - start) * 1000)
            log.info("chat_complete", extra={
                "provider": body.provider,
                "file_id": body.file_id,
                "message_count": len(body.messages),
                "duration_ms": duration_ms,
            })
        yield "data: [DONE]\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")
