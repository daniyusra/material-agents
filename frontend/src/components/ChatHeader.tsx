import type { FileInfo, Provider } from "../types";

const PROVIDER_LABEL: Record<Provider, string> = {
  anthropic: "Claude",
  openai: "GPT-4o",
};

interface ChatHeaderProps {
  fileInfo: FileInfo;
  provider: Provider;
  streaming: boolean;
  onOpenOptions: () => void;
  onChangeFile: () => void;
}

export default function ChatHeader({
  fileInfo,
  provider,
  streaming,
  onOpenOptions,
  onChangeFile,
}: ChatHeaderProps) {
  return (
    <div style={styles.header}>
      <div style={styles.left}>
        <h1 style={styles.title}>Material Agents</h1>
        <span
          style={styles.fileBadge}
          title={`${fileInfo.rows} rows · ${fileInfo.columns.length} columns`}
        >
          {fileInfo.filename}
        </span>
      </div>
      <div style={styles.right}>
        <span style={styles.providerLabel}>{PROVIDER_LABEL[provider]}</span>
        <button
          onClick={onOpenOptions}
          disabled={streaming}
          style={styles.optionsBtn}
          title="Options"
        >
          ⚙ Options
        </button>
        <button
          onClick={onChangeFile}
          disabled={streaming}
          style={styles.changeFileBtn}
        >
          Change file
        </button>
      </div>
    </div>
  );
}

const styles = {
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "1rem 0 0.5rem",
    flexWrap: "wrap" as const,
    gap: "0.5rem",
  },
  left: {
    display: "flex",
    alignItems: "center",
    gap: "0.75rem",
  },
  right: {
    display: "flex",
    alignItems: "center",
    gap: "0.5rem",
  },
  title: {
    fontSize: "1.25rem",
    fontWeight: 600,
    color: "#333",
    margin: 0,
  },
  fileBadge: {
    fontSize: "0.8rem",
    background: "#e8f0fe",
    color: "#1a56db",
    padding: "0.2rem 0.6rem",
    borderRadius: 20,
    maxWidth: 220,
    overflow: "hidden" as const,
    textOverflow: "ellipsis" as const,
    whiteSpace: "nowrap" as const,
  },
  providerLabel: {
    fontSize: "0.8rem",
    color: "#666",
    padding: "0.2rem 0.5rem",
    background: "#f5f5f5",
    borderRadius: 6,
  },
  optionsBtn: {
    padding: "0.4rem 0.75rem",
    borderRadius: 8,
    border: "1px solid #ccc",
    fontSize: "0.875rem",
    fontFamily: "inherit",
    background: "white",
    cursor: "pointer",
    color: "#333",
  },
  changeFileBtn: {
    padding: "0.4rem 0.75rem",
    borderRadius: 8,
    border: "1px solid #ccc",
    fontSize: "0.875rem",
    fontFamily: "inherit",
    background: "white",
    cursor: "pointer",
    color: "#555",
  },
} as const;
