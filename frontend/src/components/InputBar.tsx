interface InputBarProps {
  input: string;
  streaming: boolean;
  onChange: (value: string) => void;
  onSend: () => void;
}

export default function InputBar({ input, streaming, onChange, onSend }: InputBarProps) {
  const disabled = streaming || !input.trim();

  return (
    <div className="flex gap-2 py-3 pb-4 border-t border-border-dim">
      <textarea
        value={input}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); onSend(); }
        }}
        placeholder="Query the dataset… (Enter to send, Shift+Enter for newline)"
        disabled={streaming}
        rows={1}
        className="flex-1 py-3 px-4 bg-bg-surface border border-border-muted rounded-md text-text-primary text-[0.875rem] leading-[1.55] resize-none outline-none transition-[border-color] duration-[120ms] ease focus:border-accent-border disabled:opacity-45 placeholder:text-text-dim"
      />
      <button
        onClick={onSend}
        disabled={disabled}
        className="py-3 px-4 bg-accent text-bg-void font-display font-bold text-[0.82rem] tracking-[0.06em] border-none rounded-md cursor-pointer self-end whitespace-nowrap min-w-[76px] transition-opacity duration-[120ms] ease disabled:opacity-30 disabled:cursor-not-allowed enabled:hover:opacity-[0.82]"
      >
        {streaming ? "···" : "Send →"}
      </button>
    </div>
  );
}
