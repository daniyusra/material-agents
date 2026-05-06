# material-agents

A data-analysis chat app: upload a CSV/TSV/XLSX file, then ask questions about it in natural language. The backend runs sandboxed Python/pandas code and returns text answers and Plotly charts over SSE.

## Sub-projects

| Directory | Stack | README |
|---|---|---|
| `backend/` | Python · FastAPI · LangGraph · Anthropic/OpenAI | [backend/README.md](backend/README.md) |
| `frontend/` | TypeScript · React · Vite | [frontend/README.md](frontend/README.md) |

## Quick start (local)

```bash
# 1. Backend
cd backend
cp .env.example .env   # add ANTHROPIC_API_KEY
uv sync
uv run uvicorn app.main:app --reload

# 2. Frontend (new terminal)
cd frontend
nvm use 20
npm install
npm run dev            # → http://localhost:5173
```

## Deployment

| Service | Platform | Trigger |
|---|---|---|
| Backend | [Fly.io](https://fly.io) | GitHub Actions on push to `main` touching `backend/` |
| Frontend | [Vercel](https://vercel.com) | Vercel's native GitHub integration on push to `main` |

See each sub-project's README for full deployment instructions.
