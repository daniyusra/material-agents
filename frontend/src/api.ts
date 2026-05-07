import type { FileInfo, Message, PlotlyFigure, Provider } from "./types";

// In production, set VITE_API_BASE_URL to the backend origin (e.g. https://material-agents-backend.fly.dev).
// In local dev it's empty, so the Vite proxy handles /api/* requests.
const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "";

export async function uploadFile(file: File): Promise<FileInfo> {
  const form = new FormData();
  form.append("file", file);
  const res = await fetch(`${API_BASE}/api/upload`, { method: "POST", body: form });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error((body as { detail?: string }).detail ?? `HTTP ${res.status}`);
  }
  return res.json() as Promise<FileInfo>;
}

export interface ChatPayload {
  messages: Message[];
  provider: Provider;
  file_id: string | null;
  api_key?: string;
}

export async function streamChat(
  payload: ChatPayload,
  onText: (token: string) => void,
  onChart: (fig: PlotlyFigure) => void,
): Promise<void> {
  const res = await fetch(`${API_BASE}/api/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    // Accumulate across reads — a single SSE event (especially a chart JSON)
    // can span multiple chunks. Split only on the SSE event boundary (\n\n).
    buffer += decoder.decode(value, { stream: true });
    const events = buffer.split("\n\n");
    buffer = events.pop() ?? "";

    for (const event of events) {
      for (const line of event.split("\n")) {
        if (!line.startsWith("data: ")) continue;
        const data = line.slice(6);
        if (data === "[DONE]") return;
        try {
          const parsed = JSON.parse(data) as { type?: string; content: unknown };
          if (!parsed.type || parsed.type === "text") {
            onText(parsed.content as string);
          } else if (parsed.type === "chart") {
            onChart(parsed.content as PlotlyFigure);
          } else if (parsed.type === "error") {
            throw new Error(parsed.content as string);
          }
        } catch (e) {
          if (e instanceof SyntaxError) continue;
          throw e;
        }
      }
    }
  }
}
