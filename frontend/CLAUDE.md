# Frontend — TypeScript/React

## Stack

- **Framework:** React 18 + TypeScript
- **Build tool:** Vite
- **Charts:** react-plotly.js (renders Plotly figures returned by the backend)
- **No UI library** — plain inline styles

## Running

```bash
nvm use 20
npm install
npm run dev   # → http://localhost:5173
```

The Vite dev server proxies `/api` → `http://localhost:8000`, so the backend must be running.

## Key files

- `src/types.ts` — shared types (`Provider`, `Message`, `FileInfo`, `PlotlyFigure`) and constants (`PROVIDERS`, `ACCEPTED`)
- `src/api.ts` — stateless fetch helpers: `uploadFile(file)` and `streamChat(payload, onText, onChart)`
- `src/App.tsx` — state container (~100 lines); owns all state and routes between `<UploadScreen>` and `<ChatScreen>`
- `src/components/UploadScreen.tsx` — drag-and-drop / file-picker screen shown before a file is loaded
- `src/components/ChatScreen.tsx` — layout shell composing header + message list + input bar
- `src/components/ChatHeader.tsx` — filename badge, provider selector, "Change file" button
- `src/components/MessageList.tsx` — scrollable flex column of `<MessageBubble>` elements
- `src/components/MessageBubble.tsx` — renders text and optional `<ChartPanel>`
- `src/components/ChartPanel.tsx` — fixed-height Plotly wrapper + JSON download button
- `src/components/InputBar.tsx` — textarea + send button, Enter-to-send

## Screens

Two top-level screens controlled by `App.tsx`:
1. **UploadScreen** — shown when `fileInfo === null`; calls `handleFile()` on file selection/drop
2. **ChatScreen** — shown after upload; all state (messages, input, streaming, provider) lives in `App.tsx` and is passed as props

## Streaming

`api.ts:streamChat` reads SSE from `POST /api/chat`.
- Lines `data: {"type":"text","content":"..."}` call `onText(token)`
- Lines `data: {"type":"chart","content":{...}}` call `onChart(figure)`
- `data: [DONE]` signals end of stream

## Chart rendering

`ChartPanel` wraps `<Plot>` in a fixed-height container (`420px`, `overflow: hidden`) to prevent the Plotly `useResizeHandler` + `autosize` feedback loop that causes vertical overflow on scroll.

## Conventions

- All state in `App.tsx`; leaf components are stateless and receive props
- No state management library; `useState` is sufficient
- New screens are added as new top-level conditionals in `App.tsx`
- New agent UIs / panels go in `src/components/`
