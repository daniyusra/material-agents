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

  const base = "py-3 px-4 rounded-lg text-[0.875rem] leading-[1.7] whitespace-pre-wrap break-words";
  const variant = isUser
    ? "self-end bg-bg-elevated border border-border-muted text-text-primary"
    : "self-start bg-bg-surface border border-border-dim text-text-primary";
  const width = hasChart ? "max-w-[95%]" : "max-w-[75%]";
  const errStyle = isError ? "bg-error-bg border-[rgba(248,113,113,0.22)] text-error" : "";

  return (
    <div className={`${base} ${variant} ${width} ${errStyle}`}>
      {message.content || (isCurrent ? <span className="inline-block text-accent animate-cursor-blink">▊</span> : "")}
      {message.chart && <ChartPanel figure={message.chart} />}
    </div>
  );
}
