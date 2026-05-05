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
