import { useEffect, useRef, useState } from "react";

type Provider = "anthropic" | "openai";

interface Message {
  role: "user" | "assistant";
  content: string;
}

const PROVIDERS: { value: Provider; label: string }[] = [
  { value: "anthropic", label: "Claude (Anthropic)" },
  { value: "openai", label: "GPT-4o (OpenAI)" },
];

export default function App() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [streaming, setStreaming] = useState(false);
  const [provider, setProvider] = useState<Provider>("anthropic");
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

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
        body: JSON.stringify({ messages: history, provider }),
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
              updated[updated.length - 1] = {
                ...last,
                content: last.content + content,
              };
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

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <h1 style={styles.title}>Material Agents</h1>
        <select
          value={provider}
          onChange={(e) => setProvider(e.target.value as Provider)}
          disabled={streaming}
          style={styles.select}
        >
          {PROVIDERS.map((p) => (
            <option key={p.value} value={p.value}>
              {p.label}
            </option>
          ))}
        </select>
      </div>

      <div style={styles.messages}>
        {messages.length === 0 && (
          <p style={styles.empty}>Send a message to start chatting.</p>
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
            {msg.content ||
              (streaming && i === messages.length - 1 ? "▊" : "")}
          </div>
        ))}
        <div ref={bottomRef} />
      </div>

      <div style={styles.inputRow}>
        <textarea
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              send();
            }
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
  },
  title: {
    fontSize: "1.25rem",
    fontWeight: 600,
    color: "#333",
    margin: 0,
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
} as const;
