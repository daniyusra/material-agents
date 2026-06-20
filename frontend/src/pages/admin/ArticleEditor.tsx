import {
  ChangeEvent,
  KeyboardEvent,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";
import { useNavigate, useParams } from "react-router-dom";
import MDEditor, { commands, type ICommand, type TextAreaTextApi } from "@uiw/react-md-editor";
import { blogApi, type Article } from "../../api/blogApi";

// ── Helpers ───────────────────────────────────────────────────────────────────

function slugify(title: string): string {
  return title
    .normalize("NFKD")
    .replace(/[^\x00-\x7F]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80) || "article";
}

function wordCount(text: string): number {
  return text.trim() ? text.trim().split(/\s+/).length : 0;
}

// ── Sub-components ────────────────────────────────────────────────────────────

function TagsInput({
  tags,
  onChange,
}: {
  tags: string[];
  onChange: (tags: string[]) => void;
}) {
  const [input, setInput] = useState("");

  function addTag(raw: string) {
    const tag = raw.trim().toLowerCase();
    if (tag && !tags.includes(tag)) onChange([...tags, tag]);
    setInput("");
  }

  function handleKeyDown(e: KeyboardEvent<HTMLInputElement>) {
    if (e.key === "Enter" || e.key === ",") {
      e.preventDefault();
      addTag(input);
    } else if (e.key === "Backspace" && input === "" && tags.length > 0) {
      onChange(tags.slice(0, -1));
    }
  }

  return (
    <div className="flex flex-wrap gap-1.5 min-h-[38px] bg-bg-elevated border border-border-dim rounded-md px-2.5 py-1.5 focus-within:border-accent-border transition-colors cursor-text">
      {tags.map((t) => (
        <span
          key={t}
          className="flex items-center gap-1 px-2 py-0.5 bg-bg-surface border border-border-muted rounded-sm text-text-secondary text-xs font-mono"
        >
          {t}
          <button
            type="button"
            onClick={() => onChange(tags.filter((x) => x !== t))}
            className="text-text-dim hover:text-error transition-colors leading-none"
            aria-label={`Remove tag ${t}`}
          >
            ×
          </button>
        </span>
      ))}
      <input
        value={input}
        onChange={(e) => setInput(e.target.value)}
        onKeyDown={handleKeyDown}
        onBlur={() => input && addTag(input)}
        placeholder={tags.length === 0 ? "Add tags (Enter or comma to add)…" : ""}
        className="flex-1 min-w-[120px] bg-transparent text-text-primary text-xs outline-none placeholder:text-text-dim"
      />
    </div>
  );
}

function SidebarField({
  label,
  children,
  hint,
}: {
  label: string;
  children: React.ReactNode;
  hint?: string;
}) {
  return (
    <div className="flex flex-col gap-1.5">
      <label className="text-text-dim text-[0.65rem] tracking-widest uppercase font-mono">
        {label}
      </label>
      {children}
      {hint && <p className="text-text-dim text-[0.65rem] leading-relaxed">{hint}</p>}
    </div>
  );
}

// Image insert modal shown when the toolbar "Insert image" button is clicked
function ImageInsertModal({
  onInsert,
  onClose,
}: {
  onInsert: (url: string, alt: string) => void;
  onClose: () => void;
}) {
  const [file, setFile] = useState<File | null>(null);
  const [preview, setPreview] = useState("");
  const [alt, setAlt] = useState("");
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState("");

  function handleFile(e: ChangeEvent<HTMLInputElement>) {
    const f = e.target.files?.[0];
    if (!f) return;
    setFile(f);
    setPreview(URL.createObjectURL(f));
  }

  async function handleInsert() {
    if (!file || !alt.trim()) return;
    setUploading(true);
    setError("");
    try {
      const result = await blogApi.uploadMedia(file, alt.trim());
      onInsert(result.url, alt.trim());
    } catch (err: unknown) {
      setError(
        err instanceof Error ? err.message : "Upload failed. Check file size and type."
      );
    } finally {
      setUploading(false);
    }
  }

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm z-[200] flex items-center justify-center p-4">
      <div className="bg-bg-surface border border-border-muted rounded-lg p-6 w-full max-w-md shadow-2xl">
        <div className="flex items-center justify-between mb-5">
          <h2 className="font-display text-base font-semibold text-text-primary">
            Insert image
          </h2>
          <button
            onClick={onClose}
            className="text-text-dim hover:text-text-secondary transition-colors text-lg leading-none"
          >
            ×
          </button>
        </div>

        {/* File drop zone */}
        <label className="flex flex-col items-center justify-center gap-2 border-2 border-dashed border-border-muted rounded-lg p-6 cursor-pointer hover:border-accent-border transition-colors mb-4">
          {preview ? (
            <img
              src={preview}
              alt="Preview"
              className="max-h-40 rounded object-contain"
            />
          ) : (
            <>
              <span className="text-2xl text-text-dim">🖼</span>
              <span className="text-text-secondary text-sm">
                Click to choose or drop an image
              </span>
              <span className="text-text-dim text-xs">JPEG, PNG, WebP, GIF</span>
            </>
          )}
          <input
            type="file"
            accept="image/jpeg,image/png,image/webp,image/gif"
            onChange={handleFile}
            className="sr-only"
          />
        </label>

        {/* Alt text — required */}
        <div className="flex flex-col gap-1.5 mb-4">
          <label className="text-text-dim text-[0.65rem] tracking-widest uppercase font-mono">
            Alt text <span className="text-error">*</span>
          </label>
          <input
            value={alt}
            onChange={(e) => setAlt(e.target.value)}
            placeholder="Describe the image for screen readers and SEO"
            className="bg-bg-elevated border border-border-dim rounded-md px-3 py-2 text-text-primary text-sm outline-none focus:border-accent-border transition-colors"
          />
          <p className="text-text-dim text-[0.65rem]">
            Alt text is required for accessibility and SEO.
          </p>
        </div>

        {error && (
          <p className="text-error text-xs bg-error-bg rounded px-3 py-2 mb-4">{error}</p>
        )}

        <div className="flex gap-3">
          <button
            onClick={onClose}
            className="flex-1 py-2 rounded-md text-sm text-text-secondary border border-border-dim hover:border-border-muted transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleInsert}
            disabled={!file || !alt.trim() || uploading}
            className="flex-1 py-2 rounded-md text-sm font-semibold bg-accent text-bg-void hover:opacity-90 disabled:opacity-40 disabled:cursor-not-allowed transition-opacity"
          >
            {uploading ? "Uploading…" : "Insert"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Main editor ───────────────────────────────────────────────────────────────

interface FormState {
  title: string;
  slug: string;
  body: string;
  excerpt: string;
  meta_description: string;
  cover_image_url: string;
  tags: string[];
}

type SaveStatus = "idle" | "saving" | "saved" | "error";
type AutosaveStatus = "" | "Saved locally" | "Changes saved";

export default function ArticleEditor() {
  const { id } = useParams<{ id?: string }>();
  const articleId = id ? parseInt(id, 10) : null;
  const navigate = useNavigate();

  // ── State ──────────────────────────────────────────────────────────────────
  const [article, setArticle] = useState<Article | null>(null);
  const [loading, setLoading] = useState(!!articleId);
  const [saveStatus, setSaveStatus] = useState<SaveStatus>("idle");
  const [autosaveStatus, setAutosaveStatus] = useState<AutosaveStatus>("");
  const [warnings, setWarnings] = useState<string[]>([]);
  const [imageModalOpen, setImageModalOpen] = useState(false);
  const [slugEditing, setSlugEditing] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [previewOpen, setPreviewOpen] = useState(true);
  const [splitPercent, setSplitPercent] = useState(50);

  const [title, setTitle] = useState("");
  const [slug, setSlug] = useState("");
  const [body, setBody] = useState("");
  const [excerpt, setExcerpt] = useState("");
  const [metaDescription, setMetaDescription] = useState("");
  const [coverImageUrl, setCoverImageUrl] = useState("");
  const [tags, setTags] = useState<string[]>([]);

  // Ref to the MDEditor API — populated by the image insert command's execute()
  const editorApiRef = useRef<TextAreaTextApi | null>(null);
  const autosaveTimer = useRef<ReturnType<typeof setTimeout>>();
  const splitContainerRef = useRef<HTMLDivElement>(null);
  // True only after the user has actually edited something; prevents the load
  // state-setter from triggering a spurious autosave that shows a false
  // "unsaved changes" prompt on the next visit.
  const isDirty = useRef(false);
  const storageKey = `blog-draft-${articleId ?? "new"}`;

  // ── Derived ────────────────────────────────────────────────────────────────
  const wc = wordCount(body);
  const readingTime = Math.max(1, Math.ceil(wc / 200));
  const isPublished = article?.status === "published";
  const slugFrozen = article?.slug_frozen === 1;

  // ── Load existing article ──────────────────────────────────────────────────
  useEffect(() => {
    if (!articleId) return;
    // `cancelled` guards against React StrictMode running effects twice —
    // without it, two fetches both complete and `confirm()` fires twice.
    let cancelled = false;
    blogApi
      .adminGetArticle(articleId)
      .then((data) => {
        if (cancelled) return;
        setArticle(data);
        setTitle(data.title);
        setSlug(data.slug);
        setBody(data.body ?? "");
        setExcerpt(data.excerpt ?? "");
        setMetaDescription(data.meta_description ?? "");
        setCoverImageUrl(data.cover_image_url ?? "");
        setTags(data.tags ?? []);
        isDirty.current = false; // load is not a user edit

        // Check for a newer local backup
        const saved = localStorage.getItem(storageKey);
        if (saved) {
          try {
            const parsed = JSON.parse(saved) as FormState & { savedAt: number };
            if (parsed.savedAt > (data.updated_at * 1000)) {
              if (confirm("You have unsaved local changes. Restore them?")) {
                setTitle(parsed.title);
                setSlug(parsed.slug);
                setBody(parsed.body);
                setExcerpt(parsed.excerpt);
                setMetaDescription(parsed.meta_description);
                setCoverImageUrl(parsed.cover_image_url);
                setTags(parsed.tags);
              } else {
                localStorage.removeItem(storageKey);
              }
            }
          } catch { /* corrupted, ignore */ }
        }
      })
      .catch(() => { if (!cancelled) alert("Failed to load article."); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [articleId, storageKey]);

  // ── Auto-derive slug from title (while unfrozen) ───────────────────────────
  useEffect(() => {
    if (!slugFrozen && !slugEditing && !articleId) {
      setSlug(slugify(title));
    }
  }, [title, slugFrozen, slugEditing, articleId]);

  // ── Autosave to localStorage (3s debounce) ─────────────────────────────────
  const scheduleAutosave = useCallback(() => {
    if (!isDirty.current) return; // only autosave after a real user edit
    clearTimeout(autosaveTimer.current);
    autosaveTimer.current = setTimeout(() => {
      const state: FormState & { savedAt: number } = {
        title, slug, body, excerpt, meta_description: metaDescription,
        cover_image_url: coverImageUrl, tags, savedAt: Date.now(),
      };
      localStorage.setItem(storageKey, JSON.stringify(state));
      setAutosaveStatus("Saved locally");
    }, 3000);
  }, [title, slug, body, excerpt, metaDescription, coverImageUrl, tags, storageKey]);

  useEffect(() => {
    if (title || body) scheduleAutosave();
    return () => clearTimeout(autosaveTimer.current);
  }, [title, slug, body, excerpt, metaDescription, coverImageUrl, tags, scheduleAutosave]);

  // ── Save / publish ─────────────────────────────────────────────────────────
  async function save(status?: "draft" | "published") {
    setSaveStatus("saving");
    setWarnings([]);

    const fields: Parameters<typeof blogApi.adminUpdateArticle>[1] = {
      title, slug, body, excerpt,
      meta_description: metaDescription || undefined,
      cover_image_url: coverImageUrl || undefined,
      tags,
    };
    if (status) fields.status = status;

    try {
      if (articleId) {
        const { article: updated, warnings: w } = await blogApi.adminUpdateArticle(articleId, fields);
        setArticle(updated);
        setSlug(updated.slug);
        setWarnings(w);
      } else {
        // Create new article then update with full content
        const created = await blogApi.adminCreateArticle(title || "Untitled");
        const { article: updated, warnings: w } = await blogApi.adminUpdateArticle(created.id, fields);
        setArticle(updated);
        setWarnings(w);
        localStorage.removeItem(storageKey);
        navigate(`/admin/edit/${created.id}`, { replace: true });
      }
      setSaveStatus("saved");
      setAutosaveStatus("Changes saved");
      localStorage.removeItem(storageKey);
      isDirty.current = false;
      setTimeout(() => setSaveStatus("idle"), 2000);
    } catch (err: unknown) {
      setSaveStatus("error");
      alert(err instanceof Error ? err.message : "Save failed.");
    }
  }

  // ── Image insert command (custom MDEditor toolbar button) ─────────────────
  const insertImageCommand: ICommand = {
    name: "insert-image",
    keyCommand: "insertImage",
    buttonProps: { "aria-label": "Insert image", title: "Insert image" },
    icon: (
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
        <rect x="3" y="3" width="18" height="18" rx="2" />
        <circle cx="8.5" cy="8.5" r="1.5" />
        <polyline points="21 15 16 10 5 21" />
      </svg>
    ),
    execute(_state, api) {
      editorApiRef.current = api;
      setImageModalOpen(true);
    },
  };

  function handleImageInserted(url: string, alt: string) {
    editorApiRef.current?.replaceSelection(`![${alt}](${url})\n`);
    setImageModalOpen(false);
  }

  // ── Draggable split divider ────────────────────────────────────────────────
  function handleDividerMouseDown(e: React.MouseEvent) {
    e.preventDefault();
    const container = splitContainerRef.current;
    if (!container) return;
    function onMouseMove(ev: MouseEvent) {
      const rect = container!.getBoundingClientRect();
      const pct = ((ev.clientX - rect.left) / rect.width) * 100;
      setSplitPercent(Math.min(75, Math.max(25, pct)));
    }
    function onMouseUp() {
      document.removeEventListener("mousemove", onMouseMove);
      document.removeEventListener("mouseup", onMouseUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    }
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
    document.addEventListener("mousemove", onMouseMove);
    document.addEventListener("mouseup", onMouseUp);
  }

  // ── Cover image upload ─────────────────────────────────────────────────────
  async function handleCoverUpload(e: ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (!file) return;
    const alt = title || "Cover image";
    try {
      const result = await blogApi.uploadMedia(file, alt);
      setCoverImageUrl(result.url);
    } catch {
      alert("Cover image upload failed.");
    }
  }

  // ── Render ────────────────────────────────────────────────────────────────
  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <span className="text-text-dim text-sm animate-pulse">Loading article…</span>
      </div>
    );
  }

  const API_BASE = (import.meta.env.VITE_API_BASE_URL ?? "").replace(/\/$/, "");
  const siteBase = import.meta.env.VITE_SITE_BASE_URL ?? window.location.origin;

  return (
    <div className="h-[calc(100vh-3.5rem)] bg-bg-base flex flex-col overflow-hidden">
      {/* Top bar */}
      <div className="shrink-0 bg-bg-base border-b border-border-dim px-6 py-3 flex items-center justify-between gap-4 z-[90]">
        <div className="flex items-center gap-3 min-w-0">
          <button
            onClick={() => navigate("/admin")}
            className="text-text-dim hover:text-text-secondary text-sm transition-colors shrink-0"
          >
            ← Admin
          </button>
          <span className="text-border-dim">|</span>
          <span className="text-text-dim text-xs font-mono truncate">
            {autosaveStatus && (
              <span className="text-accent/70">{autosaveStatus}</span>
            )}
          </span>
        </div>

        <div className="flex items-center gap-2 shrink-0">
          {/* Word count */}
          <span className="text-text-dim text-xs font-mono hidden sm:block">
            {wc} words · {readingTime} min
          </span>

          {/* Preview toggle */}
          <button
            onClick={() => setPreviewOpen((o) => !o)}
            title={previewOpen ? "Hide preview" : "Show preview"}
            className="p-1.5 rounded-md text-text-dim hover:text-text-secondary hover:bg-bg-elevated transition-colors"
          >
            <svg width="15" height="15" viewBox="0 0 15 15" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
              <rect x="1" y="1" width="13" height="13" rx="1.5" />
              {previewOpen && <line x1="7.5" y1="1" x2="7.5" y2="14" />}
            </svg>
          </button>

          {/* Sidebar toggle */}
          <button
            onClick={() => setSidebarOpen((o) => !o)}
            title={sidebarOpen ? "Hide panel" : "Show panel"}
            className="p-1.5 rounded-md text-text-dim hover:text-text-secondary hover:bg-bg-elevated transition-colors"
          >
            <svg width="15" height="15" viewBox="0 0 15 15" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
              <rect x="1" y="1" width="13" height="13" rx="1.5" />
              <line x1="10" y1="1" x2="10" y2="14" />
              {!sidebarOpen && <line x1="12" y1="5" x2="14" y2="7.5" />}
              {!sidebarOpen && <line x1="12" y1="10" x2="14" y2="7.5" />}
            </svg>
          </button>

          {isPublished ? (
            <a
              href={`/blog/${slug}`}
              target="_blank"
              rel="noopener noreferrer"
              className="px-3 py-1.5 rounded-md text-xs text-text-secondary border border-border-dim hover:border-border-muted no-underline transition-colors"
            >
              View live ↗
            </a>
          ) : null}

          <button
            onClick={() => save("draft")}
            disabled={saveStatus === "saving"}
            className="px-3 py-1.5 rounded-md text-xs text-text-secondary border border-border-dim hover:border-border-muted disabled:opacity-40 transition-colors"
          >
            {saveStatus === "saving" ? "Saving…" : "Save draft"}
          </button>

          <button
            onClick={() => save("published")}
            disabled={saveStatus === "saving"}
            className="px-4 py-1.5 rounded-md text-xs font-semibold bg-accent text-bg-void hover:opacity-90 disabled:opacity-40 transition-opacity"
          >
            {isPublished ? "Update" : "Publish"}
          </button>
        </div>
      </div>

      {/* Warnings */}
      {warnings.length > 0 && (
        <div className="mx-6 mt-4 px-4 py-3 bg-error-bg border border-error/20 rounded-md">
          {warnings.map((w, i) => (
            <p key={i} className="text-error text-xs">⚠ {w}</p>
          ))}
        </div>
      )}

      {/* Main layout */}
      <div className="flex flex-col lg:flex-row flex-1 min-h-0">
        {/* Editor + Preview split */}
        <div ref={splitContainerRef} className="flex flex-1 min-h-0 overflow-hidden" data-color-mode="dark">

          {/* Left: editor pane */}
          <div
            className="flex flex-col overflow-y-auto"
            style={{ width: previewOpen ? `${splitPercent}%` : "100%" }}
          >
            <div className="px-6 pt-8 pb-2 shrink-0">
              {/* Title */}
              <textarea
                value={title}
                onChange={(e) => { isDirty.current = true; setTitle(e.target.value); }}
                placeholder="Article title…"
                rows={2}
                className="w-full bg-transparent border-none outline-none resize-none font-display text-3xl font-bold text-text-primary placeholder:text-text-dim leading-tight mb-5 overflow-hidden"
                style={{ minHeight: "3rem" }}
                onInput={(e) => {
                  const t = e.target as HTMLTextAreaElement;
                  t.style.height = "auto";
                  t.style.height = `${t.scrollHeight}px`;
                }}
              />

              {/* Slug */}
              <div className="flex items-center gap-2 mb-4">
                <span className="text-text-dim text-xs font-mono shrink-0">
                  {siteBase}/blog/
                </span>
                {slugFrozen && !slugEditing ? (
                  <>
                    <span className="text-accent text-xs font-mono">{slug}</span>
                    <button
                      onClick={() => setSlugEditing(true)}
                      className="text-text-dim text-xs hover:text-text-secondary transition-colors ml-1"
                      title="Override slug (creates a redirect)"
                    >
                      ✎
                    </button>
                    <span className="text-text-dim text-[0.6rem] font-mono ml-1">URL locked</span>
                  </>
                ) : (
                  <input
                    value={slug}
                    onChange={(e) => { setSlug(e.target.value.toLowerCase().replace(/[^a-z0-9-]/g, "-")); setSlugEditing(true); }}
                    className="text-accent text-xs font-mono bg-transparent border-none outline-none border-b border-accent-border focus:border-accent"
                    style={{ width: `${Math.max(slug.length + 2, 12)}ch` }}
                  />
                )}
                {slugFrozen && slugEditing && (
                  <span className="text-error text-[0.6rem] font-mono">
                    Changing creates a redirect
                  </span>
                )}
              </div>
            </div>

            {/* MDEditor */}
            <div className="px-6 pb-6">
              <MDEditor
                value={body}
                onChange={(v) => { isDirty.current = true; setBody(v ?? ""); }}
                height="calc(100vh - 240px)"
                minHeight={400}
                preview="edit"
                commands={[
                  commands.bold,
                  commands.italic,
                  commands.strikethrough,
                  commands.hr,
                  commands.divider,
                  commands.title1,
                  commands.title2,
                  commands.title3,
                  commands.divider,
                  commands.link,
                  insertImageCommand,
                  commands.divider,
                  commands.quote,
                  commands.code,
                  commands.codeBlock,
                  commands.divider,
                  commands.unorderedListCommand,
                  commands.orderedListCommand,
                  commands.checkedListCommand,
                  commands.divider,
                  commands.fullscreen,
                ]}
                extraCommands={[]}
              />
            </div>
          </div>

          {/* Draggable divider */}
          {previewOpen && (
            <div
              onMouseDown={handleDividerMouseDown}
              className="w-1 shrink-0 bg-border-dim hover:bg-accent/50 active:bg-accent cursor-col-resize transition-colors"
              title="Drag to resize"
            />
          )}

          {/* Right: preview pane */}
          {previewOpen && (
            <div
              className="overflow-y-auto bg-bg-base border-l border-border-dim"
              style={{ width: `${100 - splitPercent}%` }}
            >
              <div className="px-10 py-8 mx-auto" style={{ maxWidth: "70ch" }}>
                {body ? (
                  <MDEditor.Markdown source={body} />
                ) : (
                  <p className="text-text-dim text-sm italic pt-8">Start writing to see a preview…</p>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Sidebar */}
        <aside className={`shrink-0 border-border-dim bg-bg-surface flex flex-col gap-5 overflow-y-auto transition-all ${
          sidebarOpen
            ? "w-full lg:w-72 xl:w-80 border-t lg:border-t-0 lg:border-l px-5 py-6"
            : "hidden"
        }`}>
          {/* Status */}
          <div className="flex items-center justify-between">
            <span className="text-text-dim text-[0.65rem] tracking-widest uppercase font-mono">
              Status
            </span>
            <span
              className={`px-2 py-0.5 rounded-sm text-[0.65rem] font-mono tracking-widest uppercase ${
                isPublished
                  ? "bg-accent-glow text-accent"
                  : "bg-bg-elevated text-text-dim"
              }`}
            >
              {isPublished ? "Published" : "Draft"}
            </span>
          </div>

          {/* Excerpt */}
          <SidebarField
            label="Excerpt"
            hint="Shown in the blog list and as the default meta description."
          >
            <textarea
              value={excerpt}
              onChange={(e) => setExcerpt(e.target.value)}
              placeholder="One or two sentences summarising the article…"
              rows={3}
              className="bg-bg-elevated border border-border-dim rounded-md px-3 py-2 text-text-primary text-xs resize-none outline-none focus:border-accent-border transition-colors placeholder:text-text-dim leading-relaxed"
            />
          </SidebarField>

          {/* Tags */}
          <SidebarField label="Tags">
            <TagsInput tags={tags} onChange={setTags} />
          </SidebarField>

          {/* Cover image */}
          <SidebarField label="Cover image">
            {coverImageUrl && (
              <div className="relative mb-2">
                <img
                  src={`${API_BASE}${coverImageUrl}`}
                  alt="Cover"
                  className="w-full rounded-md object-cover aspect-[2/1]"
                />
                <button
                  onClick={() => setCoverImageUrl("")}
                  className="absolute top-1.5 right-1.5 w-6 h-6 rounded-full bg-bg-void/80 text-text-dim hover:text-error flex items-center justify-center text-xs transition-colors"
                >
                  ×
                </button>
              </div>
            )}
            <label className="flex items-center justify-center gap-2 px-3 py-2 border border-dashed border-border-muted rounded-md text-text-dim text-xs hover:border-accent-border hover:text-text-secondary cursor-pointer transition-colors">
              {coverImageUrl ? "Replace image" : "Upload cover image"}
              <input
                type="file"
                accept="image/jpeg,image/png,image/webp,image/gif"
                onChange={handleCoverUpload}
                className="sr-only"
              />
            </label>
          </SidebarField>

          {/* Meta description */}
          <SidebarField
            label="Meta description"
            hint="Overrides excerpt for search engines. ~155 chars."
          >
            <textarea
              value={metaDescription}
              onChange={(e) => setMetaDescription(e.target.value)}
              placeholder="Leave blank to use excerpt…"
              rows={2}
              maxLength={300}
              className="bg-bg-elevated border border-border-dim rounded-md px-3 py-2 text-text-primary text-xs resize-none outline-none focus:border-accent-border transition-colors placeholder:text-text-dim"
            />
            <span className="text-text-dim text-[0.6rem] font-mono self-end">
              {metaDescription.length}/300
            </span>
          </SidebarField>

          {/* Save Draft button (also in top bar; repeated here for sidebar convenience) */}
          <div className="mt-auto pt-4 border-t border-border-dim flex flex-col gap-2">
            <button
              onClick={() => save("published")}
              disabled={saveStatus === "saving"}
              className="w-full py-2.5 rounded-md text-sm font-semibold bg-accent text-bg-void hover:opacity-90 disabled:opacity-40 transition-opacity"
            >
              {saveStatus === "saving"
                ? "Saving…"
                : saveStatus === "saved"
                ? "✓ Saved"
                : isPublished
                ? "Update article"
                : "Publish article"}
            </button>
            <button
              onClick={() => save("draft")}
              disabled={saveStatus === "saving"}
              className="w-full py-2 rounded-md text-xs text-text-secondary border border-border-dim hover:border-border-muted disabled:opacity-40 transition-colors"
            >
              Save as draft
            </button>
          </div>
        </aside>
      </div>

      {/* Image insert modal */}
      {imageModalOpen && (
        <ImageInsertModal
          onInsert={handleImageInserted}
          onClose={() => setImageModalOpen(false)}
        />
      )}
    </div>
  );
}
