import { useEffect, useRef, useState } from "react";
import { uploadFile, streamChat } from "./api";
import UploadScreen from "./components/UploadScreen";
import ChatScreen from "./components/ChatScreen";
import type { FileInfo, Message, Provider } from "./types";

export default function App() {
  const [fileInfo, setFileInfo] = useState<FileInfo | null>(null);
  const [uploading, setUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);

  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [streaming, setStreaming] = useState(false);
  const [provider, setProvider] = useState<Provider>("anthropic");
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const handleFile = async (file: File) => {
    setUploadError(null);
    setUploading(true);
    try {
      setFileInfo(await uploadFile(file));
    } catch (err) {
      setUploadError(err instanceof Error ? err.message : String(err));
    } finally {
      setUploading(false);
    }
  };

  const send = async () => {
    if (!input.trim() || streaming) return;

    const userMessage: Message = { role: "user", content: input.trim() };
    const history = [...messages, userMessage];
    setMessages([...history, { role: "assistant", content: "" }]);
    setInput("");
    setStreaming(true);

    try {
      await streamChat(
        { messages: history, provider, file_id: fileInfo?.file_id ?? null },
        (token) => {
          setMessages((prev) => {
            const updated = [...prev];
            const last = updated[updated.length - 1];
            updated[updated.length - 1] = { ...last, content: last.content + token };
            return updated;
          });
        },
        (fig) => {
          setMessages((prev) => {
            const updated = [...prev];
            updated[updated.length - 1] = { ...updated[updated.length - 1], chart: fig };
            return updated;
          });
        },
      );
    } catch (err) {
      setMessages((prev) => {
        const updated = [...prev];
        updated[updated.length - 1] = {
          ...updated[updated.length - 1],
          content: `Error: ${err instanceof Error ? err.message : String(err)}`,
        };
        return updated;
      });
    } finally {
      setStreaming(false);
    }
  };

  if (!fileInfo) {
    return (
      <UploadScreen
        uploading={uploading}
        uploadError={uploadError}
        onFile={handleFile}
      />
    );
  }

  return (
    <ChatScreen
      fileInfo={fileInfo}
      messages={messages}
      input={input}
      streaming={streaming}
      provider={provider}
      bottomRef={bottomRef}
      onProviderChange={setProvider}
      onChangeFile={() => { setFileInfo(null); setMessages([]); }}
      onInputChange={setInput}
      onSend={send}
    />
  );
}
