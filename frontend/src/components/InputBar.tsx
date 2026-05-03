interface InputBarProps {
  input: string;
  streaming: boolean;
  onChange: (value: string) => void;
  onSend: () => void;
}

export default function InputBar({ input, streaming, onChange, onSend }: InputBarProps) {
  const disabled = streaming || !input.trim();

  return (
    <div style={styles.row}>
      <textarea
        value={input}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            onSend();
          }
        }}
        placeholder="Message… (Enter to send, Shift+Enter for newline)"
        disabled={streaming}
        rows={1}
        style={styles.textarea}
      />
      <button
        onClick={onSend}
        disabled={disabled}
        style={{
          ...styles.button,
          opacity: disabled ? 0.5 : 1,
          cursor: disabled ? "not-allowed" : "pointer",
        }}
      >
        {streaming ? "…" : "Send"}
      </button>
    </div>
  );
}

const styles = {
  row: {
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
