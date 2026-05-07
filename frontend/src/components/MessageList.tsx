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
    <div className="flex-1 overflow-y-auto flex flex-col gap-3 py-4">
      {messages.length === 0 && (
        <p className="text-center mt-8 font-mono text-[0.78rem] text-text-dim tracking-[0.04em] leading-[1.8]">
          Dataset loaded: <span className="text-accent-text">{filename}</span>
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
  );
}
