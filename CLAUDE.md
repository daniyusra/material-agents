# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`material-agents` is a personal AI research monorepo. Current sub-projects:

- `backend/` — Python/FastAPI service using LangChain + LangGraph + Anthropic/OpenAI for LLM agents
- `frontend/` — TypeScript/React chat UI (Vite)

## Architecture

A data-analysis chat loop:
- User uploads a tabular file (CSV/TSV/XLSX) via `POST /api/upload`
- Frontend (`localhost:5173`) sends `POST /api/chat` (with `file_id`) via Vite proxy
- Backend (`localhost:8000`) routes the request through a LangGraph data agent that can run sandboxed Python/pandas code and generate Plotly charts
- Results stream back as SSE with `type: "text"` and `type: "chart"` events
- Two LLM providers supported: Anthropic (Claude) and OpenAI (GPT-4o)
- No auth — local dev only

## Dev workflow

```bash
# Backend
cd backend && uv run uvicorn app.main:app --reload

# Frontend
cd frontend && npm run dev
```

Set `ANTHROPIC_API_KEY` (and optionally `OPENAI_API_KEY`) in `backend/.env` before starting the backend.

## Sub-project guidance

See `backend/CLAUDE.md` and `frontend/CLAUDE.md` for language-specific context.