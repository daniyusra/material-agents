import { createParser } from "eventsource-parser";
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

  await new Promise<void>((resolve, reject) => {
    const parser = createParser({
      onEvent(event) {
        if (event.data === "[DONE]") { resolve(); return; }
        try {
          const parsed = JSON.parse(event.data) as { type?: string; content: unknown };
          if (!parsed.type || parsed.type === "text") {
            onText(parsed.content as string);
          } else if (parsed.type === "chart") {
            onChart(parsed.content as PlotlyFigure);
          } else if (parsed.type === "error") {
            reject(new Error(parsed.content as string));
          }
        } catch (e) {
          if (!(e instanceof SyntaxError)) reject(e);
        }
      },
    });

    void (async () => {
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) { resolve(); break; }
          parser.feed(decoder.decode(value, { stream: true }));
        }
      } catch (e) {
        reject(e);
      }
    })();
  });
}
