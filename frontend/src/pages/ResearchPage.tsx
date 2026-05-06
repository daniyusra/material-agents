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
      <style>{`
        .rp-page {
          height: calc(100vh - var(--nav-h));
          display: flex;
          flex-direction: column;
          max-width: 900px;
          margin: 0 auto;
          padding: 0 var(--space-4);
        }

        /* Upload state */
        .rp-upload-zone {
          flex: 1;
          display: flex;
          align-items: center;
          justify-content: center;
        }
        .rp-dropzone {
          width: 100%;
          max-width: 480px;
          border: 1px dashed var(--border-muted);
          border-radius: var(--radius-lg);
          padding: var(--space-12) var(--space-8);
          text-align: center;
          transition: border-color var(--transition-fast), background var(--transition-fast);
        }
        .rp-dropzone.drag-over {
          border-color: var(--accent);
          background: var(--accent-glow);
        }
        .rp-upload-icon {
          font-family: var(--font-mono);
          font-size: 2.2rem;
          color: var(--accent);
          opacity: 0.7;
          margin-bottom: var(--space-5);
          display: block;
        }
        .rp-upload-title {
          font-family: var(--font-display);
          font-size: 1.05rem;
          font-weight: 600;
          color: var(--text-primary);
          margin-bottom: var(--space-2);
        }
        .rp-upload-hint {
          font-size: 0.85rem;
          color: var(--text-secondary);
          font-weight: 300;
          margin-bottom: var(--space-6);
          line-height: 1.55;
        }
        .rp-upload-sep {
          display: block;
          font-family: var(--font-mono);
          font-size: 0.68rem;
          color: var(--text-dim);
          letter-spacing: 0.1em;
          margin: var(--space-4) 0;
        }
        .rp-upload-btn {
          display: inline-block;
          padding: 0.65rem var(--space-6);
          background: var(--accent);
          color: var(--bg-void);
          font-family: var(--font-display);
          font-weight: 700;
          font-size: 0.85rem;
          letter-spacing: 0.05em;
          border: none;
          border-radius: var(--radius-md);
          cursor: pointer;
          transition: opacity var(--transition-fast);
        }
        .rp-upload-btn:hover { opacity: 0.85; }
        .rp-upload-formats {
          margin-top: var(--space-4);
          font-family: var(--font-mono);
          font-size: 0.68rem;
          color: var(--text-dim);
          letter-spacing: 0.1em;
        }
        .rp-upload-error {
          margin-top: var(--space-4);
          font-size: 0.82rem;
          color: var(--error);
          background: var(--error-bg);
          padding: var(--space-2) var(--space-3);
          border-radius: var(--radius-md);
          border: 1px solid rgba(248, 113, 113, 0.2);
        }

        /* Chat header */
        .rp-chat-header {
          display: flex;
          align-items: center;
          justify-content: space-between;
          padding: var(--space-3) 0;
          border-bottom: 1px solid var(--border-dim);
          flex-wrap: wrap;
          gap: var(--space-2);
        }
        .rp-header-left { display: flex; align-items: center; gap: var(--space-3); }
        .rp-header-right { display: flex; align-items: center; gap: var(--space-2); }
        .rp-file-badge {
          display: inline-flex;
          align-items: center;
          gap: var(--space-2);
          font-family: var(--font-mono);
          font-size: 0.72rem;
          color: var(--accent-text);
          background: var(--accent-glow);
          border: 1px solid var(--accent-border);
          padding: 0.2rem var(--space-3);
          border-radius: var(--radius-sm);
          max-width: 200px;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .rp-file-mark { color: var(--accent); flex-shrink: 0; }
        .rp-meta {
          font-family: var(--font-mono);
          font-size: 0.68rem;
          color: var(--text-dim);
          letter-spacing: 0.06em;
        }
        .rp-header-btn {
          padding: 0.3rem var(--space-3);
          border-radius: var(--radius-md);
          border: 1px solid var(--border-muted);
          background: transparent;
          color: var(--text-secondary);
          font-size: 0.78rem;
          letter-spacing: 0.02em;
          cursor: pointer;
          transition: color var(--transition-fast), border-color var(--transition-fast);
        }
        .rp-header-btn:hover { color: var(--text-primary); border-color: var(--accent-border); }
        .rp-header-btn:disabled { opacity: 0.35; cursor: not-allowed; }
      `}</style>

      {showOptions && (
        <OptionsModal
          provider={provider}
          apiKeys={apiKeys}
          onSave={(p, k) => { setProvider(p); setApiKeys(k); setShowOptions(false); }}
          onClose={() => setShowOptions(false)}
        />
      )}

      <div className="rp-page">
        {!fileInfo ? (
          <div className="rp-upload-zone">
            <div
              className={`rp-dropzone${dragOver ? " drag-over" : ""}`}
              onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
              onDragLeave={() => setDragOver(false)}
              onDrop={onDrop}
            >
              <span className="rp-upload-icon">⊡</span>
              <div className="rp-upload-title">Load a dataset</div>
              <p className="rp-upload-hint">
                Drag your tabular data file here<br />to begin analysis
              </p>
              <span className="rp-upload-sep">— or —</span>
              <label>
                <span className="rp-upload-btn" style={uploading ? { opacity: 0.4 } : {}}>
                  {uploading ? "Processing…" : "Select file"}
                </span>
                <input
                  type="file"
                  accept={ACCEPTED}
                  onChange={onFileInput}
                  disabled={uploading}
                  style={{ display: "none" }}
                />
              </label>
              <p className="rp-upload-formats">CSV · TSV · XLSX</p>
              {uploadError && <p className="rp-upload-error">{uploadError}</p>}
            </div>
          </div>
        ) : (
          <>
            <div className="rp-chat-header">
              <div className="rp-header-left">
                <span className="rp-file-badge">
                  <span className="rp-file-mark">◈</span>
                  {fileInfo.filename}
                </span>
                <span className="rp-meta">
                  {fileInfo.rows.toLocaleString()} rows · {fileInfo.columns.length} cols
                </span>
              </div>
              <div className="rp-header-right">
                <span className="rp-meta">{PROVIDER_LABEL[provider]}</span>
                <button
                  className="rp-header-btn"
                  onClick={() => setShowOptions(true)}
                  disabled={streaming}
                >
                  ⚙ Options
                </button>
                <button
                  className="rp-header-btn"
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
