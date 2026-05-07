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
    <div
      className="fixed inset-0 bg-[rgba(8,10,12,0.78)] flex items-center justify-center z-[1000] backdrop-blur-[2px]"
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      <div className="bg-bg-surface border border-border-muted rounded-lg p-6 w-[420px] max-w-[calc(100vw-2rem)] shadow-[0_24px_64px_rgba(0,0,0,0.6)]">
        <div className="flex items-center justify-between mb-6">
          <span className="font-display text-[1rem] font-bold text-text-primary tracking-[0.01em]">Options</span>
          <button
            onClick={onClose}
            className="bg-transparent border-none text-text-dim text-[1rem] cursor-pointer py-1 px-2 rounded-sm transition-colors duration-[120ms] ease leading-none hover:text-text-primary"
            aria-label="Close"
          >
            ✕
          </button>
        </div>

        <span className="font-mono text-[0.65rem] tracking-[0.14em] uppercase text-text-dim mb-3 block">Provider</span>
        <div className="flex flex-col gap-2">
          {PROVIDERS.map((p) => (
            <label key={p.value} className="flex items-center gap-2 text-[0.875rem] text-text-secondary cursor-pointer">
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

        <hr className="border-0 border-t border-border-dim my-5" />

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

        <div className="flex justify-end gap-2 mt-6">
          <button
            onClick={onClose}
            className="py-[0.5rem] px-4 bg-transparent border border-border-muted rounded-md text-text-secondary text-[0.85rem] cursor-pointer transition-colors duration-[120ms] ease hover:text-text-primary"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            className="py-[0.5rem] px-5 bg-accent border-none rounded-md text-bg-void font-display font-bold text-[0.85rem] tracking-[0.04em] cursor-pointer transition-opacity duration-[120ms] ease hover:opacity-85"
          >
            Save
          </button>
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
    <div className="mb-5">
      <span className="font-mono text-[0.65rem] tracking-[0.14em] uppercase text-text-dim mb-3 block">{label}</span>
      <div className="flex gap-2">
        <input
          type={show ? "text" : "password"}
          placeholder={placeholder}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className="flex-1 py-[0.5rem] px-3 bg-bg-base border border-border-muted rounded-md text-text-primary font-mono text-[0.8rem] outline-none transition-[border-color] duration-[120ms] ease focus:border-accent-border placeholder:text-text-dim"
          autoComplete="off"
          spellCheck={false}
        />
        <button
          onClick={onToggleShow}
          className="py-[0.5rem] px-3 bg-transparent border border-border-muted rounded-md text-text-secondary text-[0.78rem] cursor-pointer whitespace-nowrap transition-[color,border-color] duration-[120ms] ease hover:text-text-primary hover:border-accent-border"
          aria-label={show ? "Hide" : "Show"}
        >
          {show ? "Hide" : "Show"}
        </button>
      </div>
      {value && (
        <span className="block font-mono text-[0.7rem] text-text-dim mt-1">
          {value.slice(0, 8)}{"•".repeat(Math.min(value.length - 8, 20))}
        </span>
      )}
    </div>
  );
}
