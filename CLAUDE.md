# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`material-agents` is a personal AI research monorepo. Current sub-projects:

- `backend/` — Python/FastAPI service using LangChain + Anthropic for LLM agents
- `frontend/` — TypeScript/React chat UI (Vite)

## Architecture

The first milestone is a streaming chat loop:
- Frontend (`localhost:5173`) sends `POST /api/chat` via Vite proxy
- Backend (`localhost:8000`) streams SSE tokens from Claude via LangChain
- No auth — local dev only

## Dev workflow

```bash
# Backend
cd backend && uv run uvicorn app.main:app --reload

# Frontend
cd frontend && npm run dev
```

Set `ANTHROPIC_API_KEY` in `backend/.env` before starting the backend.

## Sub-project guidance

See `backend/CLAUDE.md` and `frontend/CLAUDE.md` for language-specific context.