interface InputBarProps {
  input: string;
  streaming: boolean;
  onChange: (value: string) => void;
  onSend: () => void;
}

export default function InputBar({ input, streaming, onChange, onSend }: InputBarProps) {
  const disabled = streaming || !input.trim();

  return (
    <>
      <style>{`
        .ib-row {
          display: flex;
          gap: var(--space-2);
          padding: var(--space-3) 0 var(--space-4);
          border-top: 1px solid var(--border-dim);
        }
        .ib-textarea {
          flex: 1;
          padding: var(--space-3) var(--space-4);
          background: var(--bg-surface);
          border: 1px solid var(--border-muted);
          border-radius: var(--radius-md);
          color: var(--text-primary);
          font-size: 0.875rem;
          line-height: 1.55;
          resize: none;
          outline: none;
          transition: border-color var(--transition-fast);
        }
        .ib-textarea::placeholder { color: var(--text-dim); }
        .ib-textarea:focus { border-color: var(--accent-border); }
        .ib-textarea:disabled { opacity: 0.45; }
        .ib-send {
          padding: var(--space-3) var(--space-4);
          background: var(--accent);
          color: var(--bg-void);
          font-family: var(--font-display);
          font-weight: 700;
          font-size: 0.82rem;
          letter-spacing: 0.06em;
          border: none;
          border-radius: var(--radius-md);
          cursor: pointer;
          align-self: flex-end;
          white-space: nowrap;
          min-width: 76px;
          transition: opacity var(--transition-fast);
        }
        .ib-send:disabled { opacity: 0.3; cursor: not-allowed; }
        .ib-send:not(:disabled):hover { opacity: 0.82; }
      `}</style>
      <div className="ib-row">
        <textarea
          value={input}
          onChange={(e) => onChange(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); onSend(); }
          }}
          placeholder="Query the dataset… (Enter to send, Shift+Enter for newline)"
          disabled={streaming}
          rows={1}
          className="ib-textarea"
        />
        <button onClick={onSend} disabled={disabled} className="ib-send">
          {streaming ? "···" : "Send →"}
        </button>
      </div>
    </>
  );
}
