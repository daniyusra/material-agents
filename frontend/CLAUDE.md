# Frontend — TypeScript/React

## Stack

- **Framework:** React 18 + TypeScript
- **Build tool:** Vite
- **Charts:** react-plotly.js (renders Plotly figures returned by the backend)
- **Styling:** Tailwind CSS v3 — no UI library, no CSS modules, no `<style>` tags

## Running

```bash
PATH=/home/daniyusra/.nvm/versions/node/v20.20.0/bin:$PATH npm install
PATH=/home/daniyusra/.nvm/versions/node/v20.20.0/bin:$PATH npm run dev   # → http://localhost:5173
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

## Design system

All tokens live in `tailwind.config.js` under `theme.extend` and are available as Tailwind classes everywhere:

| Token group | Config key | Example class |
|---|---|---|
| Backgrounds | `colors.bg-*` | `bg-bg-surface`, `bg-bg-void` |
| Borders | `colors.border-*` | `border-border-dim`, `border-border-muted` |
| Text | `colors.text-*` | `text-text-primary`, `text-text-secondary` |
| Accent | `colors.accent*` | `bg-accent`, `text-accent-text`, `border-accent-border` |
| Error | `colors.error*` | `text-error`, `bg-error-bg` |
| Fonts | `fontFamily.*` | `font-display`, `font-body`, `font-mono` |
| Radii | `borderRadius.*` | `rounded-sm` (3px), `rounded-md` (6px), `rounded-lg` (10px) |
| Nav height | `spacing.nav` | `h-nav`, `pt-nav`, `top-nav` (= 3.5rem) |
| Animations | `animation.*` | `animate-pulse-status`, `animate-cursor-blink` |

`src/styles/global.css` is the single CSS entry point. It contains only Tailwind directives, the Google Fonts import, base resets, and two `@layer components` rules for the hero pseudo-elements (dot grid + radial fade) that cannot be expressed as utilities.

## Conventions

- All state in `App.tsx`; leaf components are stateless and receive props
- No state management library; `useState` is sufficient
- New screens are added as new top-level conditionals in `App.tsx`
- New agent UIs / panels go in `src/components/`
- Styling: Tailwind classes inline in JSX; no `<style>` tags; no new CSS files
