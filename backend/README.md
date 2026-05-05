# Backend

Python/FastAPI service powering the Material Agents data analysis chat loop.

## Local development

```bash
# Install dependencies
uv sync

# Copy and fill in env vars
cp .env.example .env

# Start dev server (hot reload)
uv run uvicorn app.main:app --reload
```

Requires `ANTHROPIC_API_KEY` in `.env`. Set `OPENAI_API_KEY` too if using the OpenAI provider.

## Running tests

```bash
uv run pytest -v
```

Rate limiting is automatically disabled in the test environment.

---

## Environment variables

All variables are optional unless marked **required**.

| Variable | Default | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | — | Fallback Anthropic key when user doesn't supply one |
| `OPENAI_API_KEY` | — | Fallback OpenAI key when user doesn't supply one |
| `ALLOWED_ORIGINS` | `http://localhost:5173` | Comma-separated list of allowed CORS origins |
| `MAX_UPLOAD_MB` | `10` | Maximum file upload size in MB |
| `UPLOAD_DIR` | `./uploads` | Directory for uploaded files (use a persistent volume path in production) |
| `FILE_TTL_SECONDS` | `7200` | Seconds before an uploaded file is purged (2 hours) |
| `CLEANUP_INTERVAL_SECONDS` | `3600` | How often the cleanup task runs (1 hour) |
| `MAX_MESSAGES` | `50` | Max messages per chat request |
| `MAX_MESSAGE_LENGTH` | `8000` | Max characters per message |
| `RATELIMIT_ENABLED` | `true` | Set to `false` to disable rate limiting |
| `RATELIMIT_UPLOAD` | `10/minute` | Upload rate limit per IP |
| `RATELIMIT_CHAT` | `30/minute` | Chat rate limit per IP |
| `LOG_LEVEL` | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `SENTRY_DSN` | — | Sentry DSN for error tracking (optional) |
| `SENTRY_TRACES_SAMPLE_RATE` | `0.1` | Sentry performance tracing sample rate |
| `ENVIRONMENT` | `production` | Environment tag sent to Sentry |

---

## Deploying to Fly.io

### Prerequisites

```bash
# Install the Fly CLI (once)
curl -L https://fly.io/install.sh | sh

# Log in
fly auth login
```

### First deploy

Run these from the `backend/` directory:

```bash
# 1. Create the app (pick a unique name and your nearest region)
fly launch --name material-agents-backend --region sin --no-deploy

# 2. Create a persistent volume for uploaded files (1 GB to start)
fly volumes create uploads_data --region sin --size 1

# 3. Set required secrets
fly secrets set \
  ANTHROPIC_API_KEY="sk-ant-..." \
  OPENAI_API_KEY="sk-..." \
  ALLOWED_ORIGINS="https://your-frontend.fly.dev" \
  UPLOAD_DIR="/data/uploads"

# 4. Deploy
fly deploy
```

The app will be live at `https://material-agents-backend.fly.dev` (or your custom domain).

### Subsequent deploys

```bash
fly deploy
```

### Useful commands

```bash
fly logs                  # tail live logs
fly status                # instance health
fly ssh console           # shell into the running container
fly volumes list          # check volume usage
fly secrets list          # list secret names (values are redacted)
```

### Scaling the volume

```bash
fly volumes extend <volume-id> --size 5   # extend to 5 GB
```

---

## Production setup (Fly.io)

### HTTPS

Nothing to configure. Fly.io terminates TLS at its edge proxy — the app runs plain HTTP on port 8080 internally, and all traffic is served over HTTPS automatically. For custom domains:

```bash
fly certs add yourdomain.com
```

### CORS

Set `ALLOWED_ORIGINS` to your frontend URL(s):

```bash
fly secrets set ALLOWED_ORIGINS="https://yourapp.com"
# Multiple origins:
fly secrets set ALLOWED_ORIGINS="https://yourapp.com,https://www.yourapp.com"
```

### Persistent file storage

Uploaded files are stored on disk. On Fly.io, use a persistent volume so files survive container restarts:

```bash
# Create a volume (run once)
fly volumes create uploads_data --size 1   # 1 GB; increase as needed

# In fly.toml, mount the volume:
# [mounts]
#   source = "uploads_data"
#   destination = "/data"

# Then set UPLOAD_DIR to point at the volume:
fly secrets set UPLOAD_DIR="/data/uploads"
```

### Rate limiting

Rate limiting is in-process (per-instance memory) — no external service needed for a single-instance deployment. If you scale to multiple Fly.io machines, swap to a Redis-backed limiter (requires adding `limits[redis]` and a Redis app to your config).

### Logging

Logs are written as JSON to stdout. Fly.io ingests them automatically and makes them available via:

```bash
fly logs
```

To forward logs to an external service (Grafana Loki, Datadog, Papertrail, etc.), add a log drain in your Fly.io dashboard or via:

```bash
fly log-destinations create
```

Useful log events emitted by the app:

| Event | Fields |
|---|---|
| `startup_complete` | `restored_files` |
| `file_uploaded` | `file_id`, `filename`, `rows`, `columns` |
| `chat_complete` | `provider`, `file_id`, `message_count`, `duration_ms` |
| `chat_error` | `provider`, `error` |
| `classified` | `intent`, `chart_type`, `file_id` |
| `code_execution_failed` | `intent`, `error` |
| `files_purged` | `count` |
| `registry_rebuilt` | `restored` |
| `registry_skip_corrupt_file` | `path`, `error` |

### Error tracking (Sentry)

Create a free project at [sentry.io](https://sentry.io), then:

```bash
fly secrets set SENTRY_DSN="https://your-key@sentry.io/your-project-id"
```

Sentry integration is automatically skipped if `SENTRY_DSN` is not set.

### Health checks

The app exposes `GET /health` which returns `{"status": "ok"}`. Add it to `fly.toml`:

```toml
[[services.http_checks]]
  interval = 10000
  timeout = 2000
  grace_period = "5s"
  method = "GET"
  path = "/health"
```
