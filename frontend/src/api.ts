import type { FileInfo, Message, PlotlyFigure, Provider } from "./types";

export async function uploadFile(file: File): Promise<FileInfo> {
  const form = new FormData();
  form.append("file", file);
  const res = await fetch("/api/upload", { method: "POST", body: form });
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
}

export async function streamChat(
  payload: ChatPayload,
  onText: (token: string) => void,
  onChart: (fig: PlotlyFigure) => void,
): Promise<void> {
  const res = await fetch("/api/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);

  const reader = res.body.getReader();
  const decoder = new TextDecoder();

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    const lines = decoder.decode(value, { stream: true }).split("\n");
    for (const line of lines) {
      if (!line.startsWith("data: ")) continue;
      const payload = line.slice(6);
      if (payload === "[DONE]") return;
      try {
        const event = JSON.parse(payload) as { type?: string; content: unknown };
        if (!event.type || event.type === "text") {
          onText(event.content as string);
        } else if (event.type === "chart") {
          onChart(event.content as PlotlyFigure);
        }
      } catch {
        // ignore malformed lines
      }
    }
  }
}
