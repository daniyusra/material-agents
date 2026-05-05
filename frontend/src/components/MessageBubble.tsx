import ChartPanel from "./ChartPanel";
import type { Message } from "../types";

interface MessageBubbleProps {
  message: Message;
  isCurrent: boolean;
}

export default function MessageBubble({ message, isCurrent }: MessageBubbleProps) {
  const isUser = message.role === "user";
  const hasChart = Boolean(message.chart);
  const isError = Boolean(message.isError);

  const background = isUser ? "#0070f3" : isError ? "#fff0f0" : hasChart ? "white" : "#f0f0f0";
  const color = isUser ? "white" : isError ? "#991b1b" : "#111";
  const border = isError ? "1px solid #fca5a5" : hasChart ? "1px solid #e0e0e0" : "none";

  return (
    <div
      style={{
        ...styles.bubble,
        alignSelf: isUser ? "flex-end" : "flex-start",
        background,
        color,
        maxWidth: hasChart ? "95%" : "75%",
        border,
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
