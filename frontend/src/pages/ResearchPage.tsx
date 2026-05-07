import { useEffect, useRef, useState } from "react";
import { uploadFile, streamChat } from "../api";
import { getCookie } from "../cookies";
import OptionsModal from "../components/OptionsModal";
import MessageList from "../components/MessageList";
import InputBar from "../components/InputBar";
import { ACCEPTED } from "../types";
import type { ApiKeys, FileInfo, Message, Provider } from "../types";

const PROVIDER_LABEL: Record<Provider, string> = {
  anthropic: "Claude",
  openai: "GPT-4o",
};

export default function ResearchPage() {
  const [fileInfo, setFileInfo] = useState<FileInfo | null>(null);
  const [uploading, setUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [dragOver, setDragOver] = useState(false);

  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [streaming, setStreaming] = useState(false);
  const [provider, setProvider] = useState<Provider>("anthropic");
  const [apiKeys, setApiKeys] = useState<ApiKeys>(() => ({
    anthropic: getCookie("apikey_anthropic"),
    openai: getCookie("apikey_openai"),
  }));
  const [showOptions, setShowOptions] = useState(false);
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

  const onFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) handleFile(file);
    e.target.value = "";
  };

  const onDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const file = e.dataTransfer.files[0];
    if (file) handleFile(file);
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
        { messages: history, provider, file_id: fileInfo?.file_id ?? null, api_key: apiKeys[provider] },
        (token) =>
          setMessages((prev) => {
            const updated = [...prev];
            const last = updated[updated.length - 1];
            updated[updated.length - 1] = { ...last, content: last.content + token };
            return updated;
          }),
        (fig) =>
          setMessages((prev) => {
            const updated = [...prev];
            updated[updated.length - 1] = { ...updated[updated.length - 1], chart: fig };
            return updated;
          }),
      );
    } catch (err) {
      setMessages((prev) => {
        const updated = [...prev];
        updated[updated.length - 1] = {
          ...updated[updated.length - 1],
          content: `Error: ${err instanceof Error ? err.message : String(err)}`,
          isError: true,
        };
        return updated;
      });
    } finally {
      setStreaming(false);
    }
  };

  return (
    <>
      {showOptions && (
        <OptionsModal
          provider={provider}
          apiKeys={apiKeys}
          onSave={(p, k) => { setProvider(p); setApiKeys(k); setShowOptions(false); }}
          onClose={() => setShowOptions(false)}
        />
      )}

      <div className="h-[calc(100vh-3.5rem)] flex flex-col max-w-[900px] mx-auto px-4">
        {!fileInfo ? (
          <div className="flex-1 flex items-center justify-center">
            <div
              className={`w-full max-w-[480px] border border-dashed rounded-lg py-12 px-8 text-center transition-[border-color,background] duration-[120ms] ease ${
                dragOver ? "border-accent bg-accent-glow" : "border-border-muted"
              }`}
              onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
              onDragLeave={() => setDragOver(false)}
              onDrop={onDrop}
            >
              <span className="font-mono text-[2.2rem] text-accent opacity-70 mb-5 block">⊡</span>
              <div className="font-display text-[1.05rem] font-semibold text-text-primary mb-2">
                Load a dataset
              </div>
              <p className="text-[0.85rem] text-text-secondary font-light mb-6 leading-[1.55]">
                Drag your tabular data file here<br />to begin analysis
              </p>
              <span className="block font-mono text-[0.68rem] text-text-dim tracking-[0.1em] my-4">
                — or —
              </span>
              <label>
                <span
                  className={`inline-block py-[0.65rem] px-6 bg-accent text-bg-void font-display font-bold text-[0.85rem] tracking-[0.05em] border-none rounded-md cursor-pointer transition-opacity duration-[120ms] ease hover:opacity-85 ${uploading ? "opacity-40" : ""}`}
                >
                  {uploading ? "Processing…" : "Select file"}
                </span>
                <input
                  type="file"
                  accept={ACCEPTED}
                  onChange={onFileInput}
                  disabled={uploading}
                  className="hidden"
                />
              </label>
              <p className="mt-4 font-mono text-[0.68rem] text-text-dim tracking-[0.1em]">
                CSV · TSV · XLSX
              </p>
              {uploadError && (
                <p className="mt-4 text-[0.82rem] text-error bg-error-bg py-2 px-3 rounded-md border border-[rgba(248,113,113,0.2)]">
                  {uploadError}
                </p>
              )}
            </div>
          </div>
        ) : (
          <>
            <div className="flex items-center justify-between py-3 border-b border-border-dim flex-wrap gap-2">
              <div className="flex items-center gap-3">
                <span className="inline-flex items-center gap-2 font-mono text-[0.72rem] text-accent-text bg-accent-glow border border-accent-border py-[0.2rem] px-3 rounded-sm max-w-[200px] overflow-hidden text-ellipsis whitespace-nowrap">
                  <span className="text-accent shrink-0">◈</span>
                  {fileInfo.filename}
                </span>
                <span className="font-mono text-[0.68rem] text-text-dim tracking-[0.06em]">
                  {fileInfo.rows.toLocaleString()} rows · {fileInfo.columns.length} cols
                </span>
              </div>
              <div className="flex items-center gap-2">
                <span className="font-mono text-[0.68rem] text-text-dim tracking-[0.06em]">
                  {PROVIDER_LABEL[provider]}
                </span>
                <button
                  className="py-[0.3rem] px-3 rounded-md border border-border-muted bg-transparent text-text-secondary text-[0.78rem] tracking-[0.02em] cursor-pointer transition-[color,border-color] duration-[120ms] ease hover:text-text-primary hover:border-accent-border disabled:opacity-35 disabled:cursor-not-allowed"
                  onClick={() => setShowOptions(true)}
                  disabled={streaming}
                >
                  ⚙ Options
                </button>
                <button
                  className="py-[0.3rem] px-3 rounded-md border border-border-muted bg-transparent text-text-secondary text-[0.78rem] tracking-[0.02em] cursor-pointer transition-[color,border-color] duration-[120ms] ease hover:text-text-primary hover:border-accent-border disabled:opacity-35 disabled:cursor-not-allowed"
                  onClick={() => { setFileInfo(null); setMessages([]); }}
                  disabled={streaming}
                >
                  Change file
                </button>
              </div>
            </div>
            <MessageList
              messages={messages}
              streaming={streaming}
              filename={fileInfo.filename}
              bottomRef={bottomRef}
            />
            <InputBar input={input} streaming={streaming} onChange={setInput} onSend={send} />
          </>
        )}
      </div>
    </>
  );
}
