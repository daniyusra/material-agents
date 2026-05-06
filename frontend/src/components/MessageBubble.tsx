import ChartPanel from "./ChartPanel";
import type { Message } from "../types";

interface MessageBubbleProps {
  message: Message;
  isCurrent: boolean;
}

export default function MessageBubble({ message, isCurrent }: MessageBubbleProps) {
  const isUser = message.role === "user";
  const isError = Boolean(message.isError);
  const hasChart = Boolean(message.chart);

  const classes = [
    "bubble",
    isUser ? "bubble-user" : "bubble-assistant",
    hasChart ? "bubble-wide" : "",
    isError ? "bubble-error" : "",
  ].filter(Boolean).join(" ");

  return (
    <div className={classes}>
      {message.content || (isCurrent ? <span className="bubble-cursor">▊</span> : "")}
      {message.chart && <ChartPanel figure={message.chart} />}
    </div>
  );
}
