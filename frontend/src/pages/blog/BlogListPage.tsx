import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { blogApi, type Article } from "../../api/blogApi";

function TagBadge({ tag }: { tag: string }) {
  return (
    <span className="px-2 py-0.5 rounded-sm bg-bg-elevated text-text-dim text-[0.7rem] font-mono tracking-wide">
      {tag}
    </span>
  );
}

function ArticleCard({ article }: { article: Article }) {
  const pub = article.published_at
    ? new Date(article.published_at * 1000).toLocaleDateString("en-US", {
        year: "numeric",
        month: "long",
        day: "numeric",
      })
    : "";

  return (
    <article className="group border border-border-dim rounded-lg bg-bg-surface hover:border-accent-border hover:bg-bg-elevated transition-colors duration-150">
      {article.cover_image_url && (
        <div className="aspect-[2/1] overflow-hidden rounded-t-lg">
          <img
            src={article.cover_image_url}
            alt=""
            className="w-full h-full object-cover group-hover:scale-[1.02] transition-transform duration-300"
            loading="lazy"
          />
        </div>
      )}
      <div className="p-6">
        <div className="flex items-center gap-3 mb-3">
          {article.tags.slice(0, 3).map((t) => (
            <TagBadge key={t} tag={t} />
          ))}
        </div>

        <h2 className="font-display text-xl font-semibold text-text-primary mb-2 leading-snug">
          <Link
            to={`/blog/${article.slug}`}
            className="hover:text-accent transition-colors no-underline"
          >
            {article.title}
          </Link>
        </h2>

        {article.excerpt && (
          <p className="text-text-secondary text-sm leading-relaxed mb-4 line-clamp-3">
            {article.excerpt}
          </p>
        )}

        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 text-text-dim text-xs font-mono">
            <span>{pub}</span>
            {pub && <span>·</span>}
            <span>{article.reading_time_minutes} min read</span>
          </div>
          <Link
            to={`/blog/${article.slug}`}
            className="text-accent text-sm no-underline hover:underline"
          >
            Read →
          </Link>
        </div>
      </div>
    </article>
  );
}

export default function BlogListPage() {
  const [articles, setArticles] = useState<Article[]>([]);
  const [totalPages, setTotalPages] = useState(1);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    setLoading(true);
    blogApi
      .listArticles(page)
      .then(({ articles, total_pages }) => {
        setArticles(articles);
        setTotalPages(total_pages);
      })
      .catch(() => setError("Failed to load articles."))
      .finally(() => setLoading(false));
  }, [page]);

  return (
    <div className="max-w-[900px] mx-auto px-6 py-16">
      <header className="mb-14">
        <h1 className="font-display text-4xl font-bold text-text-primary mb-3">
          Blog
        </h1>
        <p className="text-text-secondary text-base max-w-prose">
          Articles, notes, and experiments.
        </p>
      </header>

      {loading && (
        <div className="flex items-center justify-center py-24">
          <span className="text-text-dim text-sm animate-pulse">Loading…</span>
        </div>
      )}

      {error && !loading && (
        <p className="text-error text-sm bg-error-bg rounded-md px-4 py-3">{error}</p>
      )}

      {!loading && !error && articles.length === 0 && (
        <p className="text-text-dim text-sm">No articles published yet.</p>
      )}

      {!loading && !error && articles.length > 0 && (
        <>
          <div className="grid gap-6 sm:grid-cols-2">
            {articles.map((a) => (
              <ArticleCard key={a.id} article={a} />
            ))}
          </div>

          {totalPages > 1 && (
            <div className="flex items-center justify-center gap-3 mt-12">
              <button
                disabled={page <= 1}
                onClick={() => setPage((p) => p - 1)}
                className="px-4 py-2 rounded-md text-sm text-text-secondary bg-bg-elevated border border-border-dim hover:text-text-primary hover:border-border-muted disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
              >
                ← Previous
              </button>
              <span className="text-text-dim text-sm font-mono">
                {page} / {totalPages}
              </span>
              <button
                disabled={page >= totalPages}
                onClick={() => setPage((p) => p + 1)}
                className="px-4 py-2 rounded-md text-sm text-text-secondary bg-bg-elevated border border-border-dim hover:text-text-primary hover:border-border-muted disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
              >
                Next →
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
