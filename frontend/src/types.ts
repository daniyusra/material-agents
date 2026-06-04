export type Provider = "anthropic" | "openai";

export interface PlotlyFigure {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data: any[];
  layout: Record<string, unknown>;
  [key: string]: unknown;
}

export interface Message {
  role: "user" | "assistant";
  content: string;
  chart?: PlotlyFigure;
  isError?: boolean;
}

export interface FileInfo {
  file_id: string;
  filename: string;
  rows: number;
  columns: string[];
}

export const PROVIDERS: { value: Provider; label: string }[] = [
  { value: "anthropic", label: "Claude (Anthropic)" },
  { value: "openai", label: "GPT-4o (OpenAI)" },
];

export const ACCEPTED = ".csv,.tsv,.xlsx,.xls";

export interface ApiKeys {
  anthropic?: string;
  openai?: string;
}

// ── WhatsApp bot types ────────────────────────────────────────────────────────

export interface WaGroup {
  group_jid: string;
  message_count: number;
  last_activity: number;
}

export interface WaTodo {
  id: number;
  text: string;
  owner: string | null;
  group_jid: string;
  created_at: number;
  done: number;
}

export interface WaGoal {
  id: number;
  title: string;
  description: string | null;
  group_jid: string;
  created_at: number;
  target_date: number | null;
  done: number;
  created_by: string | null;
}

export interface WaReminder {
  id: number;
  text: string;
  group_jid: string;
  due_at: number;
  sent: number;
  created_by: string | null;
  created_at: number;
}
