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
    <>
      <style>{`
        .om-overlay {
          position: fixed;
          inset: 0;
          background: rgba(8, 10, 12, 0.78);
          display: flex;
          align-items: center;
          justify-content: center;
          z-index: 1000;
          backdrop-filter: blur(2px);
        }
        .om-modal {
          background: var(--bg-surface);
          border: 1px solid var(--border-muted);
          border-radius: var(--radius-lg);
          padding: var(--space-6);
          width: 420px;
          max-width: calc(100vw - 2rem);
          box-shadow: 0 24px 64px rgba(0, 0, 0, 0.6);
        }
        .om-header {
          display: flex;
          align-items: center;
          justify-content: space-between;
          margin-bottom: var(--space-6);
        }
        .om-title {
          font-family: var(--font-display);
          font-size: 1rem;
          font-weight: 700;
          color: var(--text-primary);
          letter-spacing: 0.01em;
        }
        .om-close {
          background: none;
          border: none;
          color: var(--text-dim);
          font-size: 1rem;
          cursor: pointer;
          padding: var(--space-1) var(--space-2);
          border-radius: var(--radius-sm);
          transition: color var(--transition-fast);
          line-height: 1;
        }
        .om-close:hover { color: var(--text-primary); }
        .om-label {
          font-family: var(--font-mono);
          font-size: 0.65rem;
          letter-spacing: 0.14em;
          text-transform: uppercase;
          color: var(--text-dim);
          margin-bottom: var(--space-3);
          display: block;
        }
        .om-radio-group { display: flex; flex-direction: column; gap: var(--space-2); }
        .om-radio-label {
          display: flex;
          align-items: center;
          gap: var(--space-2);
          font-size: 0.875rem;
          color: var(--text-secondary);
          cursor: pointer;
        }
        .om-radio-label input[type="radio"] { accent-color: var(--accent); }
        .om-divider {
          border: none;
          border-top: 1px solid var(--border-dim);
          margin: var(--space-5) 0;
        }
        .om-key-field { margin-bottom: var(--space-5); }
        .om-key-row { display: flex; gap: var(--space-2); }
        .om-key-input {
          flex: 1;
          padding: 0.5rem var(--space-3);
          background: var(--bg-base);
          border: 1px solid var(--border-muted);
          border-radius: var(--radius-md);
          color: var(--text-primary);
          font-family: var(--font-mono);
          font-size: 0.8rem;
          outline: none;
          transition: border-color var(--transition-fast);
        }
        .om-key-input:focus { border-color: var(--accent-border); }
        .om-key-input::placeholder { color: var(--text-dim); }
        .om-show-btn {
          padding: 0.5rem var(--space-3);
          background: transparent;
          border: 1px solid var(--border-muted);
          border-radius: var(--radius-md);
          color: var(--text-secondary);
          font-size: 0.78rem;
          cursor: pointer;
          white-space: nowrap;
          transition: color var(--transition-fast), border-color var(--transition-fast);
        }
        .om-show-btn:hover { color: var(--text-primary); border-color: var(--accent-border); }
        .om-key-hint {
          display: block;
          font-family: var(--font-mono);
          font-size: 0.7rem;
          color: var(--text-dim);
          margin-top: var(--space-1);
        }
        .om-actions {
          display: flex;
          justify-content: flex-end;
          gap: var(--space-2);
          margin-top: var(--space-6);
        }
        .om-cancel {
          padding: 0.5rem var(--space-4);
          background: transparent;
          border: 1px solid var(--border-muted);
          border-radius: var(--radius-md);
          color: var(--text-secondary);
          font-size: 0.85rem;
          cursor: pointer;
          transition: color var(--transition-fast);
        }
        .om-cancel:hover { color: var(--text-primary); }
        .om-save {
          padding: 0.5rem var(--space-5);
          background: var(--accent);
          border: none;
          border-radius: var(--radius-md);
          color: var(--bg-void);
          font-family: var(--font-display);
          font-weight: 700;
          font-size: 0.85rem;
          letter-spacing: 0.04em;
          cursor: pointer;
          transition: opacity var(--transition-fast);
        }
        .om-save:hover { opacity: 0.85; }
      `}</style>

      <div className="om-overlay" onClick={(e) => e.target === e.currentTarget && onClose()}>
        <div className="om-modal">
          <div className="om-header">
            <span className="om-title">Options</span>
            <button onClick={onClose} className="om-close" aria-label="Close">✕</button>
          </div>

          <span className="om-label">Provider</span>
          <div className="om-radio-group">
            {PROVIDERS.map((p) => (
              <label key={p.value} className="om-radio-label">
                <input
                  type="radio"
                  name="provider"
                  value={p.value}
                  checked={draft.provider === p.value}
                  onChange={() => setDraft((d) => ({ ...d, provider: p.value }))}
                />
                {p.label}
              </label>
            ))}
          </div>

          <hr className="om-divider" />

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

          <div className="om-actions">
            <button onClick={onClose} className="om-cancel">Cancel</button>
            <button onClick={handleSave} className="om-save">Save</button>
          </div>
        </div>
      </div>
    </>
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
    <div className="om-key-field">
      <span className="om-label">{label}</span>
      <div className="om-key-row">
        <input
          type={show ? "text" : "password"}
          placeholder={placeholder}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className="om-key-input"
          autoComplete="off"
          spellCheck={false}
        />
        <button onClick={onToggleShow} className="om-show-btn" aria-label={show ? "Hide" : "Show"}>
          {show ? "Hide" : "Show"}
        </button>
      </div>
      {value && (
        <span className="om-key-hint">
          {value.slice(0, 8)}{"•".repeat(Math.min(value.length - 8, 20))}
        </span>
      )}
    </div>
  );
}
