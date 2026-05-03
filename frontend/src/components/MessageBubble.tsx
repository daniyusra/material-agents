import ChartPanel from "./ChartPanel";
import type { Message } from "../types";

interface MessageBubbleProps {
  message: Message;
  isCurrent: boolean;
}

export default function MessageBubble({ message, isCurrent }: MessageBubbleProps) {
  const isUser = message.role === "user";
  const hasChart = Boolean(message.chart);

  return (
    <div
      style={{
        ...styles.bubble,
        alignSelf: isUser ? "flex-end" : "flex-start",
        background: isUser ? "#0070f3" : hasChart ? "white" : "#f0f0f0",
        color: isUser ? "white" : "#111",
        maxWidth: hasChart ? "95%" : "75%",
        border: hasChart ? "1px solid #e0e0e0" : "none",
      }}
    >
      {message.content || (isCurrent ? "▊" : "")}
      {message.chart && <ChartPanel figure={message.chart} />}
    </div>
  );
}

const styles = {
  bubble: {
    padding: "0.75rem 1rem",
    borderRadius: 12,
    fontSize: "0.95rem",
    lineHeight: 1.5,
    whiteSpace: "pre-wrap" as const,
    wordBreak: "break-word" as const,
  },
} as const;
