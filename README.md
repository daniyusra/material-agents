# material-agents

A personal AI research monorepo with two features:

1. **Research Terminal** — upload a CSV/TSV/XLSX file, ask questions in natural language; backend runs sandboxed pandas code and returns text + Plotly charts over SSE.
2. **WhatsApp Group Bot** — tracks todos, goals, and reminders extracted from group conversations, with AI-powered recap and a React dashboard.

## Sub-projects

| Directory | Stack |
|---|---|
| `backend/` | Python · FastAPI · LangGraph · Anthropic / OpenAI |
| `frontend/` | TypeScript · React · Vite |

---

## Dev setup

### Prerequisites

- Python 3.12+ with [uv](https://docs.astral.sh/uv/)
- Node 20 (via nvm or direct install)
- Docker Desktop (required for the WhatsApp bot)

### 1. Backend

```bash
cd backend
cp .env.example .env   # fill in API keys (see .env.example)
uv sync
uv run uvicorn app.main:app --reload --host 0.0.0.0
```

`--host 0.0.0.0` is required so the Evolution API Docker container can reach the backend via `host.docker.internal`.

### 2. Frontend

```bash
cd frontend
nvm use 20   # or: PATH=/home/<you>/.nvm/versions/node/v20.20.0/bin:$PATH
npm install
npm run dev   # → http://localhost:5173
```

### 3. WhatsApp bot (Evolution API)

The bot uses [Evolution API](https://github.com/evolution-foundation/evolution-api) as a self-hosted WhatsApp bridge.

#### Start the stack

```bash
docker compose up -d
```

This starts Evolution API v2.3.6, PostgreSQL 16, and Redis. The compose file already includes all required settings to avoid the QR-generation loop bug present in older versions (disabled Redis cache, local cache enabled, history sync off, correct phone version string).

Wait ~15 seconds for Postgres to become healthy, then confirm:

```bash
docker compose ps
docker compose logs evolution-api --tail 20
```

Look for `Server is listening on port 8080`.

#### Create a WhatsApp instance

```bash
curl -s -X POST http://localhost:8080/instance/create \
  -H "apikey: changeme" \
  -H "Content-Type: application/json" \
  -d '{
    "instanceName": "mybot",
    "integration": "WHATSAPP-BAILEYS",
    "qrcode": true
  }' | python3 -m json.tool
```

The response includes a `qrcode.base64` field with the QR image. Decode and scan it with WhatsApp → Linked Devices → Link a device.

To fetch the QR again at any time:

```bash
curl -s http://localhost:8080/instance/connect/mybot \
  -H "apikey: changeme" | python3 -m json.tool
```

#### Poll for connection

```bash
watch -n2 'curl -s http://localhost:8080/instance/connectionState/mybot -H "apikey: changeme"'
```

Wait for `"state": "open"`.

#### Register the webhook

```bash
curl -s -X POST http://localhost:8080/webhook/set/mybot \
  -H "apikey: changeme" \
  -H "Content-Type: application/json" \
  -d '{
    "webhook": {
      "enabled": true,
      "url": "http://host.docker.internal:8000/whatsapp/webhook",
      "events": ["MESSAGES_UPSERT"]
    }
  }'
```

`host.docker.internal` resolves to the host machine from inside the Docker container. The `extra_hosts` entry in `docker-compose.yml` makes this work on Linux/WSL2 as well as Mac/Windows Docker Desktop.

#### Backend env vars

`backend/.env` needs these in addition to the LLM API keys:

```env
EVOLUTION_API_URL=http://localhost:8080
EVOLUTION_INSTANCE=mybot
EVOLUTION_API_KEY=changeme
```

#### Sanity check

```bash
# Should return your connected group(s) once messages arrive
curl -s http://localhost:8000/api/whatsapp/groups | python3 -m json.tool

# Inject a synthetic webhook event to test the pipeline without a real phone
curl -s -X POST http://localhost:8000/whatsapp/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "event": "messages.upsert",
    "data": {
      "key": {
        "remoteJid": "1234567890@g.us",
        "fromMe": false,
        "id": "test-001",
        "participant": "60123456789@s.whatsapp.net"
      },
      "pushName": "Test User",
      "message": {"conversation": "!todo buy milk"},
      "messageTimestamp": 1700000000
    }
  }'
```

---

## WhatsApp bot capabilities

The bot only acts on **WhatsApp group** messages (DMs are ignored).

### Commands

| Command | What it does |
|---|---|
| `!recap` | AI summary of the last 100 group messages, sent back to the group |
| `!todo <task>` | Add a todo (optionally assign with `@name`) |
| `!todo list` | List all open todos for this group |
| `!todo done <id>` | Mark todo #id complete |
| `!remind <text> in <duration>` | Set a reminder — supports `30m`, `2h`, `1d`, `1w` |
| `!goal <title>` | Add a group goal |
| `!goal list` | List active goals |
| `!goal done <id>` | Mark goal #id complete |

### Passive extraction

Every 10 non-command messages, GPT-4o scans the batch and silently creates todos for anything that reads like an action item. No command needed.

### Scheduler

- **Reminders:** checked every 5 minutes; fires the reminder message to the group when due.
- **Weekly recap:** runs automatically every Monday at 08:00 UTC.

### Dashboard

`http://localhost:5173/whatsapp` — view todos, goals, reminders per group and trigger a recap manually.

### Storage

All data is stored in a local SQLite file (`backend/whatsapp.db`). No external database required for the bot itself — PostgreSQL is only used by Evolution API.

---

## Production deployment

### Prerequisites

- [flyctl](https://fly.io/docs/hands-on/install-flyctl/) installed and authenticated (`fly auth login`)
- A server (VPS, Hetzner, DigitalOcean, etc.) to run Evolution API — it needs a stable public IP

---

### Step 1 — Deploy the backend to Fly.io

```bash
cd backend

# First time only: create the app and its persistent volume
fly apps create material-agents-backend
fly volumes create data --region nrt --size 1   # 1 GB; adjust region if needed

# Set all secrets (never committed to git)
fly secrets set \
  ANTHROPIC_API_KEY=sk-ant-... \
  OPENAI_API_KEY=sk-proj-... \
  EVOLUTION_API_URL=https://<your-evolution-server>:8080 \
  EVOLUTION_INSTANCE=mybot \
  EVOLUTION_API_KEY=<your-evolution-api-key> \
  WHATSAPP_WEBHOOK_SECRET=<random-string-you-choose>

# Deploy
fly deploy
```

The `fly.toml` already has:
- `UPLOAD_DIR=/data/uploads` and `WHATSAPP_DB_PATH=/data/whatsapp.db` — both on the persistent volume
- `min_machines_running = 1` — keeps the process alive for APScheduler (reminders, weekly recap)
- Health check at `/health`

Your backend will be live at `https://material-agents-backend.fly.dev`.

---

### Step 2 — Deploy Evolution API on your server

Copy `docker-compose.yml` to your server and run it there. Change `AUTHENTICATION_API_KEY` to a strong secret:

```bash
scp docker-compose.yml user@your-server:~/evolution/
ssh user@your-server
cd ~/evolution

# Edit AUTHENTICATION_API_KEY in docker-compose.yml, then:
docker compose up -d
```

Expose port 8080 (or put nginx in front). The Evolution API will be reachable at `https://<your-server>:8080` or your domain.

---

### Step 3 — Connect WhatsApp on the production server

Run these against your production Evolution API URL:

```bash
EVOURL=https://<your-evolution-server>:8080
APIKEY=<your-evolution-api-key>

# Create instance
curl -s -X POST $EVOURL/instance/create \
  -H "apikey: $APIKEY" \
  -H "Content-Type: application/json" \
  -d '{"instanceName": "mybot", "integration": "WHATSAPP-BAILEYS", "qrcode": true}' \
  | python3 -m json.tool

# Scan QR with WhatsApp → Linked Devices → Link a device
curl -s $EVOURL/instance/connect/mybot -H "apikey: $APIKEY" | python3 -m json.tool

# Confirm connected
curl -s $EVOURL/instance/connectionState/mybot -H "apikey: $APIKEY"
```

---

### Step 4 — Point the webhook at your Fly backend

```bash
curl -s -X POST $EVOURL/webhook/set/mybot \
  -H "apikey: $APIKEY" \
  -H "Content-Type: application/json" \
  -d '{
    "webhook": {
      "enabled": true,
      "url": "https://material-agents-backend.fly.dev/whatsapp/webhook",
      "events": ["MESSAGES_UPSERT"],
      "headers": {
        "x-webhook-secret": "<same-WHATSAPP_WEBHOOK_SECRET-you-set-above>"
      }
    }
  }'
```

---

### Step 5 — Deploy the frontend to Vercel

In Vercel dashboard: import the repo, set root directory to `frontend`, and add this environment variable:

```
VITE_API_BASE_URL=https://material-agents-backend.fly.dev
```

Then deploy. The frontend will proxy API calls to your Fly backend.

---

### Subsequent deploys

```bash
cd backend && fly deploy
```

That's it — secrets and volumes persist across deploys.
