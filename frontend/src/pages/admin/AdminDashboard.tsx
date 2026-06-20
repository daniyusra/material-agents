import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { blogApi, type Article } from "../../api/blogApi";
import { useAuth } from "../../contexts/AuthContext";

function StatusBadge({ status }: { status: "draft" | "published" }) {
  return status === "published" ? (
    <span className="px-2 py-0.5 rounded-sm bg-accent-glow text-accent text-[0.65rem] font-mono tracking-widest uppercase">
      Published
    </span>
  ) : (
    <span className="px-2 py-0.5 rounded-sm bg-bg-elevated text-text-dim text-[0.65rem] font-mono tracking-widest uppercase">
      Draft
    </span>
  );
}

export default function AdminDashboard() {
  const { logout } = useAuth();
  const navigate = useNavigate();
  const [articles, setArticles] = useState<Article[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [deleting, setDeleting] = useState<number | null>(null);

  function load() {
    setLoading(true);
    blogApi
      .adminListArticles()
      .then(({ articles }) => setArticles(articles))
      .catch(() => setError("Failed to load articles."))
      .finally(() => setLoading(false));
  }

  useEffect(load, []);

  async function handleToggleStatus(article: Article) {
    const newStatus = article.status === "published" ? "draft" : "published";
    try {
      await blogApi.adminUpdateArticle(article.id, { status: newStatus });
      load();
    } catch {
      alert("Failed to update status.");
    }
  }

  async function handleDelete(article: Article) {
    if (!confirm(`Delete "${article.title}"? This cannot be undone.`)) return;
    setDeleting(article.id);
    try {
      await blogApi.adminDeleteArticle(article.id);
      setArticles((prev) => prev.filter((a) => a.id !== article.id));
    } catch {
      alert("Failed to delete article.");
    } finally {
      setDeleting(null);
    }
  }

  async function handleLogout() {
    await logout();
    navigate("/admin/login", { replace: true });
  }

  const published = articles.filter((a) => a.status === "published").length;
  const drafts = articles.filter((a) => a.status === "draft").length;

  return (
    <div className="max-w-[1000px] mx-auto px-6 py-12">
      {/* Header */}
      <div className="flex items-center justify-between mb-10">
        <div>
          <h1 className="font-display text-2xl font-bold text-text-primary">
            Blog Admin
          </h1>
          <p className="text-text-dim text-sm mt-0.5 font-mono">
            {published} published · {drafts} draft{drafts !== 1 ? "s" : ""}
          </p>
        </div>
        <div className="flex items-center gap-3">
          <Link
            to="/blog"
            className="px-3 py-1.5 rounded-md text-sm text-text-secondary border border-border-dim hover:border-border-muted hover:text-text-primary no-underline transition-colors"
          >
            View blog →
          </Link>
          <button
            onClick={handleLogout}
            className="px-3 py-1.5 rounded-md text-sm text-text-dim border border-border-dim hover:border-border-muted hover:text-text-secondary transition-colors"
          >
            Sign out
          </button>
          <Link
            to="/admin/new"
            className="px-4 py-2 rounded-md text-sm font-semibold bg-accent text-bg-void hover:opacity-90 no-underline transition-opacity"
          >
            + New article
          </Link>
        </div>
      </div>

      {loading && (
        <div className="flex items-center justify-center py-20">
          <span className="text-text-dim text-sm animate-pulse">Loading…</span>
        </div>
      )}

      {error && !loading && (
        <p className="text-error bg-error-bg rounded-md px-4 py-3 text-sm">{error}</p>
      )}

      {!loading && !error && articles.length === 0 && (
        <div className="text-center py-20">
          <p className="text-text-dim mb-4">No articles yet.</p>
          <Link
            to="/admin/new"
            className="text-accent no-underline hover:underline text-sm"
          >
            Write your first article →
          </Link>
        </div>
      )}

      {!loading && articles.length > 0 && (
        <div className="border border-border-dim rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border-dim bg-bg-elevated">
                <th className="text-left px-5 py-3 text-text-dim font-mono text-xs tracking-wide uppercase">
                  Title
                </th>
                <th className="text-left px-4 py-3 text-text-dim font-mono text-xs tracking-wide uppercase w-28">
                  Status
                </th>
                <th className="text-left px-4 py-3 text-text-dim font-mono text-xs tracking-wide uppercase w-36 hidden md:table-cell">
                  Date
                </th>
                <th className="px-4 py-3 w-40" />
              </tr>
            </thead>
            <tbody>
              {articles.map((article, i) => {
                const date = article.published_at
                  ? new Date(article.published_at * 1000).toLocaleDateString("en-US", {
                      month: "short",
                      day: "numeric",
                      year: "numeric",
                    })
                  : new Date(article.updated_at * 1000).toLocaleDateString("en-US", {
                      month: "short",
                      day: "numeric",
                      year: "numeric",
                    });

                return (
                  <tr
                    key={article.id}
                    className={`border-b border-border-dim last:border-0 ${
                      i % 2 === 0 ? "bg-bg-base" : "bg-bg-surface"
                    } hover:bg-bg-elevated transition-colors`}
                  >
                    <td className="px-5 py-3.5">
                      <Link
                        to={`/admin/edit/${article.id}`}
                        className="text-text-primary no-underline hover:text-accent transition-colors font-medium"
                      >
                        {article.title}
                      </Link>
                      {article.excerpt && (
                        <p className="text-text-dim text-xs mt-0.5 line-clamp-1">
                          {article.excerpt}
                        </p>
                      )}
                    </td>
                    <td className="px-4 py-3.5">
                      <StatusBadge status={article.status} />
                    </td>
                    <td className="px-4 py-3.5 text-text-dim font-mono text-xs hidden md:table-cell">
                      {date}
                    </td>
                    <td className="px-4 py-3.5">
                      <div className="flex items-center justify-end gap-2">
                        <Link
                          to={`/admin/edit/${article.id}`}
                          className="px-2.5 py-1 rounded text-xs text-text-secondary border border-border-dim hover:border-accent-border hover:text-accent no-underline transition-colors"
                        >
                          Edit
                        </Link>
                        <button
                          onClick={() => handleToggleStatus(article)}
                          className="px-2.5 py-1 rounded text-xs text-text-secondary border border-border-dim hover:border-border-muted hover:text-text-primary transition-colors"
                        >
                          {article.status === "published" ? "Unpublish" : "Publish"}
                        </button>
                        <button
                          onClick={() => handleDelete(article)}
                          disabled={deleting === article.id}
                          className="px-2.5 py-1 rounded text-xs text-error border border-error/20 hover:bg-error-bg disabled:opacity-40 transition-colors"
                        >
                          {deleting === article.id ? "…" : "Delete"}
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
