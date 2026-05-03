# Backend — Python/FastAPI

## Stack

- **Framework:** FastAPI with Uvicorn
- **LLM:** LangChain + `langchain-anthropic` → Claude (`claude-opus-4-7`)
- **Package manager:** `uv` (preferred) or `pip`

## Running

```bash
# Install deps
uv sync          # or: pip install -e ".[dev]"

# Start dev server
uv run uvicorn app.main:app --reload
```

Requires `ANTHROPIC_API_KEY` in `.env` (copy from `.env.example`).

## Key files

- `app/main.py` — FastAPI app, CORS, `/api/chat` SSE endpoint
- `app/agents/chat.py` — LangChain chain; `stream_chat()` yields text chunks

## Streaming protocol

`POST /api/chat` accepts `{"messages": [{"role": "user"|"assistant", "content": "..."}]}` and returns `text/event-stream` with:

```
data: {"content": "<chunk>"}
data: [DONE]
```

## Conventions

- Async throughout (`async def`, `astream`)
- No global state beyond the `ChatAnthropic` model instance in `chat.py`
- Add new agent types as new files under `app/agents/`
