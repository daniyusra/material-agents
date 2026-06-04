# material-agents

A data-analysis chat app: upload a CSV/TSV/XLSX file, then ask questions about it in natural language. The backend runs sandboxed Python/pandas code and returns text answers and Plotly charts over SSE.

It also includes a **WhatsApp group bot** that tracks todos, goals, and reminders extracted from group conversations, with an AI-powered recap and a dashboard frontend.

## Sub-projects

| Directory | Stack | README |
|---|---|---|
| `backend/` | Python · FastAPI · LangGraph · Anthropic/OpenAI | [backend/README.md](backend/README.md) |
| `frontend/` | TypeScript · React · Vite | [frontend/README.md](frontend/README.md) |

---

## Quick start — Research Terminal (local)

```bash
# 1. Backend
cd backend
cp .env.example .env   # add ANTHROPIC_API_KEY and/or OPENAI_API_KEY
uv sync
uv run uvicorn app.main:app --reload

# 2. Frontend (new terminal)
cd frontend
nvm use 20
npm install
npm run dev            # → http://localhost:5173
```

---

## Quick start — WhatsApp Group Bot (local)

The bot uses [Evolution API](https://github.com/EvolutionAPI/evolution-api) as a self-hosted WhatsApp bridge running in Docker. All intelligence (AI, storage, scheduling) lives in the Python backend.

### What "API key" means here

Evolution API is your own Docker container — there is no third-party account. The `AUTHENTICATION_API_KEY` is **a password you choose yourself** when starting the container. You then set the same value as `EVOLUTION_API_KEY` in `backend/.env` so your Python backend can authenticate its calls to the container.

### Step 1 — Start Evolution API

```bash
docker run -d \
  --name evolution-api \
  -p 8080:8080 \
  -e AUTHENTICATION_API_KEY=changeme \
  atendai/evolution-api:latest
```

> Replace `changeme` with any secret string you like.

### Step 2 — Create a WhatsApp instance and scan the QR

Open `http://localhost:8080/manager` in your browser. Create an instance named `default`, then click **Connect** and scan the QR code with the WhatsApp account you want the bot to use.

Alternatively, via curl:

```bash
# Create instance
curl -s -X POST http://localhost:8080/instance/create \
  -H "apikey: changeme" \
  -H "Content-Type: application/json" \
  -d '{"instanceName": "default", "integration": "WHATSAPP-BAILEYS"}'

# Get QR code (returns a base64 PNG — paste into a QR viewer)
curl -s http://localhost:8080/instance/connect/default \
  -H "apikey: changeme"
```

### Step 3 — Configure the webhook

Evolution API needs to call your Python backend when messages arrive. Since the container can't reach `localhost:8000` directly, use `host.docker.internal` (Mac/Windows) or `172.17.0.1` (Linux):

```bash
# Mac / Windows Docker Desktop
WEBHOOK_HOST=host.docker.internal

# Linux
WEBHOOK_HOST=172.17.0.1

curl -s -X POST http://localhost:8080/webhook/set/default \
  -H "apikey: changeme" \
  -H "Content-Type: application/json" \
  -d "{
    \"webhook\": {
      \"enabled\": true,
      \"url\": \"http://${WEBHOOK_HOST}:8000/whatsapp/webhook\",
      \"events\": [\"MESSAGES_UPSERT\"]
    }
  }"
```

> **ngrok alternative:** if you want to test with a phone on a different network, run `ngrok http 8000` and use the ngrok URL instead.

### Step 4 — Configure the backend

Add these to `backend/.env` (alongside your existing `OPENAI_API_KEY`):

```env
EVOLUTION_API_URL=http://localhost:8080
EVOLUTION_INSTANCE=default
EVOLUTION_API_KEY=changeme          # same value as AUTHENTICATION_API_KEY above
```

### Step 5 — Start the backend and frontend

```bash
# Backend
cd backend
uv sync   # first time only, or after adding apscheduler/httpx
uv run uvicorn app.main:app --reload

# Frontend (new terminal)
cd frontend
npm install
npm run dev   # → http://localhost:5173/whatsapp
```

### Step 6 — Add the bot to a WhatsApp group

Add the phone number you connected in Step 2 to a WhatsApp group. The bot will start receiving messages immediately.

### Bot commands (type these in the WhatsApp group)

| Command | What it does |
|---|---|
| `!recap` | AI summary of recent messages, sent to the group |
| `!todo <task>` | Add an action item (use `@name` to assign an owner) |
| `!todo list` | List open todos |
| `!todo done <id>` | Mark a todo complete |
| `!remind <text> in 2h` | Set a reminder — supports `30m`, `2h`, `1d`, `1w` |
| `!goal <title>` | Track a group goal |
| `!goal list` | List active goals |
| `!goal done <id>` | Mark a goal complete |

Todos are also extracted **passively** from normal conversation every 10 messages via GPT-4o.

The dashboard at `http://localhost:5173/whatsapp` shows all open todos, goals, reminders, and lets you trigger a recap manually.

---

## Deployment

| Service | Platform | Trigger |
|---|---|---|
| Backend | [Fly.io](https://fly.io) | GitHub Actions on push to `main` touching `backend/` |
| Frontend | [Vercel](https://vercel.com) | Vercel's native GitHub integration on push to `main` |

For the WhatsApp bot in production, run Evolution API on the same server as the backend (or any server reachable from both), and set `EVOLUTION_API_URL` to its address.

See each sub-project's README for full deployment instructions.
