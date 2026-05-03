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
    <div style={styles.list}>
      {messages.length === 0 && (
        <p style={styles.empty}>
          Ask something about <strong>{filename}</strong>.
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
  );
}

const styles = {
  list: {
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
} as const;
