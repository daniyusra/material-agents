import type { RefObject } from "react";
import ChatHeader from "./ChatHeader";
import MessageList from "./MessageList";
import InputBar from "./InputBar";
import type { FileInfo, Message, Provider } from "../types";

interface ChatScreenProps {
  fileInfo: FileInfo;
  messages: Message[];
  input: string;
  streaming: boolean;
  provider: Provider;
  bottomRef: RefObject<HTMLDivElement>;
  onProviderChange: (p: Provider) => void;
  onChangeFile: () => void;
  onInputChange: (value: string) => void;
  onSend: () => void;
}

export default function ChatScreen({
  fileInfo,
  messages,
  input,
  streaming,
  provider,
  bottomRef,
  onProviderChange,
  onChangeFile,
  onInputChange,
  onSend,
}: ChatScreenProps) {
  return (
    <div style={styles.container}>
      <ChatHeader
        fileInfo={fileInfo}
        provider={provider}
        streaming={streaming}
        onProviderChange={onProviderChange}
        onChangeFile={onChangeFile}
      />
      <MessageList
        messages={messages}
        streaming={streaming}
        filename={fileInfo.filename}
        bottomRef={bottomRef}
      />
      <InputBar
        input={input}
        streaming={streaming}
        onChange={onInputChange}
        onSend={onSend}
      />
    </div>
  );
}

const styles = {
  container: {
    display: "flex",
    flexDirection: "column" as const,
    height: "100vh",
    maxWidth: 900,
    margin: "0 auto",
    padding: "0 1rem",
  },
} as const;
