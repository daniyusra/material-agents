import type { RefObject } from "react";
import MessageBubble from "./MessageBubble";
import type { Message } from "../types";

interface MessageListProps {
  messages: Message[];
  streaming: boolean;
  filename: string;
  bottomRef: RefObject<HTMLDivElement>;
}

export default function MessageList({ messages, streaming, filename, bottomRef }: MessageListProps) {
  return (
    <>
      <style>{`
        .ml-list {
          flex: 1;
          overflow-y: auto;
          display: flex;
          flex-direction: column;
          gap: var(--space-3);
          padding: var(--space-4) 0;
        }
        .ml-empty {
          text-align: center;
          margin-top: var(--space-8);
          font-family: var(--font-mono);
          font-size: 0.78rem;
          color: var(--text-dim);
          letter-spacing: 0.04em;
          line-height: 1.8;
        }
        .ml-empty-file { color: var(--accent-text); }

        /* Bubble styles defined here to avoid duplication across N MessageBubble instances */
        .bubble {
          padding: var(--space-3) var(--space-4);
          border-radius: var(--radius-lg);
          font-size: 0.875rem;
          line-height: 1.7;
          white-space: pre-wrap;
          word-break: break-word;
        }
        .bubble-user {
          align-self: flex-end;
          background: var(--bg-elevated);
          border: 1px solid var(--border-muted);
          color: var(--text-primary);
          max-width: 75%;
        }
        .bubble-assistant {
          align-self: flex-start;
          background: var(--bg-surface);
          border: 1px solid var(--border-dim);
          color: var(--text-primary);
          max-width: 75%;
        }
        .bubble-wide { max-width: 95%; }
        .bubble-error {
          background: var(--error-bg);
          border-color: rgba(248, 113, 113, 0.22);
          color: var(--error);
        }
        .bubble-cursor {
          display: inline-block;
          color: var(--accent);
          animation: cursor-blink 1s step-end infinite;
        }
        @keyframes cursor-blink {
          0%, 100% { opacity: 1; }
          50% { opacity: 0; }
        }

        /* Chart panel styles (defined once here since ChartPanel renders inside bubbles) */
        .cp-wrapper { margin-top: var(--space-3); }
        .cp-container {
          width: 100%;
          overflow: hidden;
          border-radius: var(--radius-md);
          border: 1px solid var(--border-dim);
          background: var(--bg-base);
        }
        .cp-dl-btn {
          margin-top: var(--space-2);
          padding: 0.28rem var(--space-3);
          background: transparent;
          border: 1px solid var(--border-muted);
          border-radius: var(--radius-sm);
          font-family: var(--font-mono);
          font-size: 0.68rem;
          letter-spacing: 0.06em;
          color: var(--text-dim);
          cursor: pointer;
          transition: color var(--transition-fast), border-color var(--transition-fast);
        }
        .cp-dl-btn:hover { color: var(--text-secondary); border-color: var(--accent-border); }
      `}</style>
      <div className="ml-list">
        {messages.length === 0 && (
          <p className="ml-empty">
            Dataset loaded: <span className="ml-empty-file">{filename}</span>
            <br />Ask a question to begin analysis
          </p>
        )}
        {messages.map((msg, i) => (
          <MessageBubble
            key={i}
            message={msg}
            isCurrent={streaming && i === messages.length - 1}
          />
        ))}
        <div ref={bottomRef} />
      </div>
    </>
  );
}
