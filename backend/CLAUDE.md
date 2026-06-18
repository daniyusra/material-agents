# Backend — Python/FastAPI

## Stack

- **Framework:** FastAPI with Uvicorn
- **LLM:** LangChain + LangGraph; providers: `langchain-anthropic` (Claude `claude-opus-4-7`) and `langchain-openai` (GPT-4o)
- **Data:** pandas for DataFrame operations; Plotly for chart generation
- **Database:** PostgreSQL via SQLAlchemy 2.x async (`asyncpg` driver) + Alembic migrations
- **Package manager:** `uv` (preferred) or `pip`

## Running

```bash
# Install deps
uv sync

# Start Postgres (Docker required)
docker compose up -d app_postgres   # from repo root

# Apply migrations
uv run alembic upgrade head

# Start dev server
uv run uvicorn app.main:app --reload
```

Requires `ANTHROPIC_API_KEY` and `DATABASE_URL` in `.env` (copy from `.env.example`). Set `OPENAI_API_KEY` too if using the OpenAI provider.

## Key files

- `app/main.py` — FastAPI app, CORS, `/api/upload` and `/api/chat` SSE endpoints
- `app/storage.py` — in-memory file registry with TTL-based expiry (2 h); supports CSV/TSV/XLSX/XLS
- `app/agents/chat.py` — simple LangChain chain; `stream_chat()` yields text chunks (no file)
- `app/agents/data_agent.py` — LangGraph agent for file-backed Q&A; classifies questions, executes pandas/viz code, synthesises answers
- `app/agents/_sandbox_runner.py` — subprocess target for sandboxed code execution (restricted builtins + AST guard + isolated env)
- `app/blog/models.py` — SQLAlchemy 2.x declarative models (`Article`, `SlugRedirect`, `Media`)
- `app/blog/database.py` — async engine + `get_db()` FastAPI dependency + `open_db()` context manager
- `app/blog/crud.py` — all async DB operations; called by the router, accepts `AsyncSession`
- `app/blog/router.py` — blog HTTP routes; injects `AsyncSession` via `Depends(get_db)`
- `alembic/` — migration scripts; run with `uv run alembic upgrade head`

## API endpoints

### `POST /api/upload`
Accepts `multipart/form-data` with a single `file` field (CSV/TSV/XLSX/XLS).
Returns `{ file_id, filename, rows, columns[] }`.

### `POST /api/chat`
```json
{ "messages": [{"role": "user"|"assistant", "content": "..."}], "provider": "anthropic"|"openai", "file_id": "<uuid>|null" }
```
Returns `text/event-stream`:
```
data: {"type": "text",  "content": "<chunk>"}
data: {"type": "chart", "content": <plotly-figure-json>}
data: [DONE]
```

## Data agent (LangGraph)

`data_agent.py` implements a LangGraph `StateGraph` with these nodes:
1. **classify** — determines `route`: `DATA_ONLY`, `DATA_VIZ`, or `GENERAL`
2. **generate_code** — writes pandas or Plotly Express code
3. **execute** — runs code through the 3-layer sandbox (AST guard → restricted builtins → subprocess)
4. **synthesize** — produces a conversational answer citing actual computed values / descriptive stats

Code execution safety (3 layers):
- **Layer 1:** restricted builtins — `exec()` gets an explicit allowlist, no `open`/`eval`/`__import__`
- **Layer 2:** AST guard — rejects `import` statements and blocked attribute access before execution
- **Layer 3:** subprocess isolation — runs in a child process with 15 s timeout and a stripped environment (no API keys)

## Conventions

- Async throughout (`async def`, `astream`)
- Add new agent types as new files under `app/agents/`
- Tests live in `backend/tests/`; run with `uv run pytest -v`
