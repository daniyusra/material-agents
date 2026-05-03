import { useEffect, useRef, useState } from "react";

type Provider = "anthropic" | "openai";

interface Message {
  role: "user" | "assistant";
  content: string;
}

interface FileInfo {
  file_id: string;
  filename: string;
  rows: number;
  columns: string[];
}

const PROVIDERS: { value: Provider; label: string }[] = [
  { value: "anthropic", label: "Claude (Anthropic)" },
  { value: "openai", label: "GPT-4o (OpenAI)" },
];

const ACCEPTED = ".csv,.tsv,.xlsx,.xls";

export default function App() {
  const [fileInfo, setFileInfo] = useState<FileInfo | null>(null);
  const [uploading, setUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);

  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [streaming, setStreaming] = useState(false);
  const [provider, setProvider] = useState<Provider>("anthropic");
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  // ── Upload ────────────────────────────────────────────────────────────────

  const handleFile = async (file: File) => {
    setUploadError(null);
    setUploading(true);
    const form = new FormData();
    form.append("file", file);
    try {
      const res = await fetch("/api/upload", { method: "POST", body: form });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail ?? `HTTP ${res.status}`);
      }
      const data: FileInfo = await res.json();
      setFileInfo(data);
    } catch (err) {
      setUploadError(err instanceof Error ? err.message : String(err));
    } finally {
      setUploading(false);
    }
  };

  const onFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) handleFile(file);
    e.target.value = "";
  };

  const onDrop = (e: React.DragEvent) => {
    e.preventDefault();
    const file = e.dataTransfer.files[0];
    if (file) handleFile(file);
  };

  // ── Chat ──────────────────────────────────────────────────────────────────

  const send = async () => {
    if (!input.trim() || streaming) return;

    const userMessage: Message = { role: "user", content: input.trim() };
    const history = [...messages, userMessage];
    setMessages([...history, { role: "assistant", content: "" }]);
    setInput("");
    setStreaming(true);

    try {
      const res = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          messages: history,
          provider,
          file_id: fileInfo?.file_id ?? null,
        }),
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
          if (payload === "[DONE]") break;
          try {
            const { content } = JSON.parse(payload) as { content: string };
            setMessages((prev) => {
              const updated = [...prev];
              const last = updated[updated.length - 1];
              updated[updated.length - 1] = { ...last, content: last.content + content };
              return updated;
            });
          } catch {
            // ignore malformed lines
          }
        }
      }
    } catch (err) {
      setMessages((prev) => {
        const updated = [...prev];
        updated[updated.length - 1] = {
          ...updated[updated.length - 1],
          content: `Error: ${err instanceof Error ? err.message : String(err)}`,
        };
        return updated;
      });
    } finally {
      setStreaming(false);
    }
  };

  // ── Upload screen ─────────────────────────────────────────────────────────

  if (!fileInfo) {
    return (
      <div style={styles.container}>
        <div style={styles.header}>
          <h1 style={styles.title}>Material Agents</h1>
        </div>
        <div style={styles.uploadWrapper}>
          <div
            style={styles.dropzone}
            onDragOver={(e) => e.preventDefault()}
            onDrop={onDrop}
          >
            <p style={styles.dropzoneHint}>
              Drag & drop a CSV, TSV, or Excel file here
            </p>
            <p style={styles.dropzoneOr}>or</p>
            <label style={styles.fileLabel}>
              {uploading ? "Uploading…" : "Choose file"}
              <input
                type="file"
                accept={ACCEPTED}
                onChange={onFileInput}
                disabled={uploading}
                style={{ display: "none" }}
              />
            </label>
            {uploadError && <p style={styles.error}>{uploadError}</p>}
          </div>
        </div>
      </div>
    );
  }

  // ── Chat screen ───────────────────────────────────────────────────────────

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <div style={styles.headerLeft}>
          <h1 style={styles.title}>Material Agents</h1>
          <span style={styles.fileBadge} title={`${fileInfo.rows} rows · ${fileInfo.columns.length} columns`}>
            {fileInfo.filename}
          </span>
        </div>
        <div style={styles.headerRight}>
          <select
            value={provider}
            onChange={(e) => setProvider(e.target.value as Provider)}
            disabled={streaming}
            style={styles.select}
          >
            {PROVIDERS.map((p) => (
              <option key={p.value} value={p.value}>{p.label}</option>
            ))}
          </select>
          <button
            onClick={() => { setFileInfo(null); setMessages([]); }}
            disabled={streaming}
            style={styles.changeFileBtn}
          >
            Change file
          </button>
        </div>
      </div>

      <div style={styles.messages}>
        {messages.length === 0 && (
          <p style={styles.empty}>Ask something about <strong>{fileInfo.filename}</strong>.</p>
        )}
        {messages.map((msg, i) => (
          <div
            key={i}
            style={{
              ...styles.bubble,
              alignSelf: msg.role === "user" ? "flex-end" : "flex-start",
              background: msg.role === "user" ? "#0070f3" : "#f0f0f0",
              color: msg.role === "user" ? "white" : "#111",
            }}
          >
            {msg.content || (streaming && i === messages.length - 1 ? "▊" : "")}
          </div>
        ))}
        <div ref={bottomRef} />
      </div>

      <div style={styles.inputRow}>
        <textarea
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); }
          }}
          placeholder="Message… (Enter to send, Shift+Enter for newline)"
          disabled={streaming}
          rows={1}
          style={styles.textarea}
        />
        <button
          onClick={send}
          disabled={streaming || !input.trim()}
          style={{
            ...styles.button,
            opacity: streaming || !input.trim() ? 0.5 : 1,
            cursor: streaming || !input.trim() ? "not-allowed" : "pointer",
          }}
        >
          {streaming ? "…" : "Send"}
        </button>
      </div>
    </div>
  );
}

const styles = {
  container: {
    display: "flex",
    flexDirection: "column" as const,
    height: "100vh",
    maxWidth: 800,
    margin: "0 auto",
    padding: "0 1rem",
  },
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "1rem 0 0.5rem",
    flexWrap: "wrap" as const,
    gap: "0.5rem",
  },
  headerLeft: {
    display: "flex",
    alignItems: "center",
    gap: "0.75rem",
  },
  headerRight: {
    display: "flex",
    alignItems: "center",
    gap: "0.5rem",
  },
  title: {
    fontSize: "1.25rem",
    fontWeight: 600,
    color: "#333",
    margin: 0,
  },
  fileBadge: {
    fontSize: "0.8rem",
    background: "#e8f0fe",
    color: "#1a56db",
    padding: "0.2rem 0.6rem",
    borderRadius: 20,
    maxWidth: 220,
    overflow: "hidden" as const,
    textOverflow: "ellipsis" as const,
    whiteSpace: "nowrap" as const,
  },
  select: {
    padding: "0.4rem 0.75rem",
    borderRadius: 8,
    border: "1px solid #ccc",
    fontSize: "0.875rem",
    fontFamily: "inherit",
    background: "white",
    cursor: "pointer",
  },
  changeFileBtn: {
    padding: "0.4rem 0.75rem",
    borderRadius: 8,
    border: "1px solid #ccc",
    fontSize: "0.875rem",
    fontFamily: "inherit",
    background: "white",
    cursor: "pointer",
    color: "#555",
  },
  messages: {
    flex: 1,
    overflowY: "auto" as const,
    display: "flex",
    flexDirection: "column" as const,
    gap: "0.75rem",
    padding: "0.75rem 0",
  },
  empty: {
    color: "#999",
    textAlign: "center" as const,
    marginTop: "2rem",
    fontSize: "0.9rem",
  },
  bubble: {
    maxWidth: "75%",
    padding: "0.75rem 1rem",
    borderRadius: 12,
    fontSize: "0.95rem",
    lineHeight: 1.5,
    whiteSpace: "pre-wrap" as const,
    wordBreak: "break-word" as const,
  },
  inputRow: {
    display: "flex",
    gap: "0.5rem",
    padding: "0.75rem 0 1rem",
  },
  textarea: {
    flex: 1,
    padding: "0.75rem",
    borderRadius: 8,
    border: "1px solid #ccc",
    fontSize: "0.95rem",
    resize: "none" as const,
    fontFamily: "inherit",
    lineHeight: 1.5,
  },
  button: {
    padding: "0.75rem 1.5rem",
    borderRadius: 8,
    background: "#0070f3",
    color: "white",
    border: "none",
    fontSize: "0.95rem",
    fontWeight: 500,
    alignSelf: "flex-end" as const,
  },
  // upload screen
  uploadWrapper: {
    flex: 1,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
  },
  dropzone: {
    border: "2px dashed #ccc",
    borderRadius: 16,
    padding: "3rem 2rem",
    textAlign: "center" as const,
    width: "100%",
    maxWidth: 420,
  },
  dropzoneHint: {
    color: "#555",
    fontSize: "1rem",
    margin: "0 0 0.5rem",
  },
  dropzoneOr: {
    color: "#aaa",
    fontSize: "0.85rem",
    margin: "0.5rem 0",
  },
  fileLabel: {
    display: "inline-block",
    padding: "0.6rem 1.5rem",
    borderRadius: 8,
    background: "#0070f3",
    color: "white",
    fontSize: "0.95rem",
    cursor: "pointer",
    fontWeight: 500,
  },
  error: {
    color: "#d32f2f",
    fontSize: "0.875rem",
    marginTop: "1rem",
  },
} as const;
