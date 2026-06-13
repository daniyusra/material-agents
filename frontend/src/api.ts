import { createParser } from "eventsource-parser";
import type { FileInfo, Message, PlotlyFigure, Provider, WaGoal, WaGroup, WaReminder, WaTodo } from "./types";

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

// ── WhatsApp bot API ──────────────────────────────────────────────────────────

async function _json<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, init);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error((body as { detail?: string }).detail ?? `HTTP ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export const waApi = {
  groups: () => _json<WaGroup[]>("/api/whatsapp/groups"),

  todos: (groupJid: string, done?: boolean) => {
    const params = new URLSearchParams({ group_jid: groupJid });
    if (done !== undefined) params.set("done", String(done));
    return _json<WaTodo[]>(`/api/whatsapp/todos?${params}`);
  },

  createTodo: (group_jid: string, text: string, owner?: string) =>
    _json<WaTodo>("/api/whatsapp/todos", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ group_jid, text, owner: owner ?? null }),
    }),

  completeTodo: (id: number) =>
    _json<{ status: string }>(`/api/whatsapp/todos/${id}/done`, { method: "POST" }),

  goals: (groupJid: string, done?: boolean) => {
    const params = new URLSearchParams({ group_jid: groupJid });
    if (done !== undefined) params.set("done", String(done));
    return _json<WaGoal[]>(`/api/whatsapp/goals?${params}`);
  },

  createGoal: (group_jid: string, title: string) =>
    _json<WaGoal>("/api/whatsapp/goals", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ group_jid, title }),
    }),

  completeGoal: (id: number) =>
    _json<{ status: string }>(`/api/whatsapp/goals/${id}/done`, { method: "POST" }),

  reminders: (groupJid: string, sent?: boolean) => {
    const params = new URLSearchParams({ group_jid: groupJid });
    if (sent !== undefined) params.set("sent", String(sent));
    return _json<WaReminder[]>(`/api/whatsapp/reminders?${params}`);
  },

  recap: (group_jid: string) =>
    _json<{ recap: string }>("/api/whatsapp/recap", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ group_jid }),
    }),
};
