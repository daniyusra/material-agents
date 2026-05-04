import { useState } from "react";
import { setCookie, deleteCookie } from "../cookies";
import { PROVIDERS } from "../types";
import type { ApiKeys, Provider } from "../types";

interface OptionsModalProps {
  provider: Provider;
  apiKeys: ApiKeys;
  onSave: (provider: Provider, apiKeys: ApiKeys) => void;
  onClose: () => void;
}

export default function OptionsModal({ provider, apiKeys, onSave, onClose }: OptionsModalProps) {
  const [draft, setDraft] = useState({
    provider,
    anthropic: apiKeys.anthropic ?? "",
    openai: apiKeys.openai ?? "",
  });
  const [showKey, setShowKey] = useState({ anthropic: false, openai: false });

  const handleSave = () => {
    const newKeys: ApiKeys = {
      anthropic: draft.anthropic || undefined,
      openai: draft.openai || undefined,
    };

    if (newKeys.anthropic) setCookie("apikey_anthropic", newKeys.anthropic);
    else deleteCookie("apikey_anthropic");

    if (newKeys.openai) setCookie("apikey_openai", newKeys.openai);
    else deleteCookie("apikey_openai");

    onSave(draft.provider, newKeys);
  };

  return (
    <div style={styles.overlay} onClick={(e) => e.target === e.currentTarget && onClose()}>
      <div style={styles.modal}>
        <div style={styles.modalHeader}>
          <span style={styles.modalTitle}>Options</span>
          <button onClick={onClose} style={styles.closeBtn} aria-label="Close">✕</button>
        </div>

        <div style={styles.section}>
          <div style={styles.sectionLabel}>Provider</div>
          <div style={styles.radioGroup}>
            {PROVIDERS.map((p) => (
              <label key={p.value} style={styles.radioLabel}>
                <input
                  type="radio"
                  name="provider"
                  value={p.value}
                  checked={draft.provider === p.value}
                  onChange={() => setDraft((d) => ({ ...d, provider: p.value }))}
                  style={{ marginRight: "0.4rem" }}
                />
                {p.label}
              </label>
            ))}
          </div>
        </div>

        <div style={styles.divider} />

        <KeyField
          label="Anthropic API Key"
          placeholder="sk-ant-..."
          value={draft.anthropic}
          show={showKey.anthropic}
          onChange={(v) => setDraft((d) => ({ ...d, anthropic: v }))}
          onToggleShow={() => setShowKey((s) => ({ ...s, anthropic: !s.anthropic }))}
        />

        <KeyField
          label="OpenAI API Key"
          placeholder="sk-..."
          value={draft.openai}
          show={showKey.openai}
          onChange={(v) => setDraft((d) => ({ ...d, openai: v }))}
          onToggleShow={() => setShowKey((s) => ({ ...s, openai: !s.openai }))}
        />

        <div style={styles.actions}>
          <button onClick={onClose} style={styles.cancelBtn}>Cancel</button>
          <button onClick={handleSave} style={styles.saveBtn}>Save</button>
        </div>
      </div>
    </div>
  );
}

interface KeyFieldProps {
  label: string;
  placeholder: string;
  value: string;
  show: boolean;
  onChange: (v: string) => void;
  onToggleShow: () => void;
}

function KeyField({ label, placeholder, value, show, onChange, onToggleShow }: KeyFieldProps) {
  return (
    <div style={styles.keyField}>
      <label style={styles.keyLabel}>{label}</label>
      <div style={styles.keyInputRow}>
        <input
          type={show ? "text" : "password"}
          placeholder={placeholder}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          style={styles.keyInput}
          autoComplete="off"
          spellCheck={false}
        />
        <button onClick={onToggleShow} style={styles.showBtn} aria-label={show ? "Hide" : "Show"}>
          {show ? "Hide" : "Show"}
        </button>
      </div>
      {value && (
        <span style={styles.keyHint}>
          {value.slice(0, 8)}{"•".repeat(Math.min(value.length - 8, 20))}
        </span>
      )}
    </div>
  );
}

const styles = {
  overlay: {
    position: "fixed" as const,
    inset: 0,
    background: "rgba(0,0,0,0.45)",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    zIndex: 1000,
  },
  modal: {
    background: "white",
    borderRadius: 12,
    padding: "1.5rem",
    width: 420,
    maxWidth: "calc(100vw - 2rem)",
    boxShadow: "0 8px 32px rgba(0,0,0,0.18)",
  },
  modalHeader: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    marginBottom: "1.25rem",
  },
  modalTitle: {
    fontSize: "1.1rem",
    fontWeight: 600,
    color: "#111",
  },
  closeBtn: {
    background: "none",
    border: "none",
    fontSize: "1rem",
    cursor: "pointer",
    color: "#888",
    padding: "0.2rem 0.4rem",
    borderRadius: 4,
  },
  section: {
    marginBottom: "1rem",
  },
  sectionLabel: {
    fontSize: "0.8rem",
    fontWeight: 600,
    color: "#555",
    textTransform: "uppercase" as const,
    letterSpacing: "0.06em",
    marginBottom: "0.5rem",
  },
  radioGroup: {
    display: "flex",
    flexDirection: "column" as const,
    gap: "0.35rem",
  },
  radioLabel: {
    display: "flex",
    alignItems: "center",
    fontSize: "0.9rem",
    color: "#222",
    cursor: "pointer",
  },
  divider: {
    height: 1,
    background: "#eee",
    margin: "1rem 0",
  },
  keyField: {
    marginBottom: "1rem",
  },
  keyLabel: {
    display: "block",
    fontSize: "0.8rem",
    fontWeight: 600,
    color: "#555",
    textTransform: "uppercase" as const,
    letterSpacing: "0.06em",
    marginBottom: "0.4rem",
  },
  keyInputRow: {
    display: "flex",
    gap: "0.5rem",
  },
  keyInput: {
    flex: 1,
    padding: "0.45rem 0.7rem",
    borderRadius: 8,
    border: "1px solid #ccc",
    fontSize: "0.875rem",
    fontFamily: "monospace",
    outline: "none",
  },
  showBtn: {
    padding: "0.45rem 0.7rem",
    borderRadius: 8,
    border: "1px solid #ccc",
    background: "white",
    fontSize: "0.8rem",
    cursor: "pointer",
    color: "#555",
    whiteSpace: "nowrap" as const,
  },
  keyHint: {
    fontSize: "0.75rem",
    color: "#999",
    marginTop: "0.3rem",
    display: "block",
    fontFamily: "monospace",
  },
  actions: {
    display: "flex",
    justifyContent: "flex-end",
    gap: "0.5rem",
    marginTop: "1.5rem",
  },
  cancelBtn: {
    padding: "0.5rem 1rem",
    borderRadius: 8,
    border: "1px solid #ccc",
    background: "white",
    fontSize: "0.875rem",
    cursor: "pointer",
    color: "#555",
    fontFamily: "inherit",
  },
  saveBtn: {
    padding: "0.5rem 1.25rem",
    borderRadius: 8,
    border: "none",
    background: "#0070f3",
    color: "white",
    fontSize: "0.875rem",
    cursor: "pointer",
    fontFamily: "inherit",
    fontWeight: 600,
  },
} as const;
