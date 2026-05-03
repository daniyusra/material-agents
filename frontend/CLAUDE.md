# Frontend — TypeScript/React

## Stack

- **Framework:** React 18 + TypeScript
- **Build tool:** Vite
- **No UI library** — plain inline styles for now

## Running

```bash
npm install
npm run dev   # → http://localhost:5173
```

The Vite dev server proxies `/api` → `http://localhost:8000`, so the backend must be running.

## Key files

- `src/App.tsx` — entire chat UI (single component for now)
- `vite.config.ts` — proxy config

## Streaming

`App.tsx` reads SSE from `POST /api/chat` using the Fetch `ReadableStream` API.
Lines starting with `data: ` are parsed as JSON `{"content": "..."}`.
`data: [DONE]` signals end of stream.

## Conventions

- Keep it simple — one component until complexity warrants splitting
- No state management library; `useState` is sufficient
- New agent UIs go in `src/` as sibling components
