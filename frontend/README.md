# Frontend

TypeScript/React chat UI for Material Agents, built with Vite.

## Local development

```bash
nvm use 20
npm install
npm run dev   # → http://localhost:5173
```

The Vite dev server proxies `/api` → `http://localhost:8000`, so the backend must be running locally.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `VITE_API_BASE_URL` | _(empty)_ | Backend origin in production (e.g. `https://material-agents-backend.fly.dev`). Leave unset locally — the Vite proxy handles it. |

Set this in a `.env.local` file for local overrides, or in your hosting platform's environment variable settings for production.

## Deploying to Vercel (recommended)

Vercel handles builds and deploys automatically via its native GitHub integration — no GitHub Actions workflow needed.

### First deploy

1. Go to [vercel.com](https://vercel.com) → **Add New Project** → import your GitHub repo.
2. Set **Root Directory** to `frontend`.
3. Vercel auto-detects Vite — build command (`npm run build`) and output directory (`dist`) are set for you.
4. Add the environment variable `VITE_API_BASE_URL` with your Fly.io backend URL (e.g. `https://material-agents-backend.fly.dev`).
5. Click **Deploy**.

### Subsequent deploys

Vercel redeploys automatically on every push to `main`. No manual steps needed.

### Custom domain

Add a domain in **Vercel → Project → Settings → Domains**, then update the backend's `ALLOWED_ORIGINS` secret to include it:

```bash
fly secrets set ALLOWED_ORIGINS="https://your-custom-domain.com" --app material-agents-backend
```

## CORS

The backend only allows requests from origins listed in its `ALLOWED_ORIGINS` secret. After you have a Vercel URL, set it on the backend:

```bash
fly secrets set ALLOWED_ORIGINS="https://your-app.vercel.app" --app material-agents-backend
```
