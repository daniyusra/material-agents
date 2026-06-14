import { useEffect, useState, useRef } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import DOMPurify from "dompurify";
import hljs from "highlight.js";
import { blogApi, type Article } from "../../api/blogApi";

function TagBadge({ tag }: { tag: string }) {
  return (
    <span className="px-2 py-0.5 rounded-sm bg-bg-elevated text-text-dim text-[0.7rem] font-mono tracking-wide">
      {tag}
    </span>
  );
}

function ArticleBody({ html }: { html: string }) {
  const ref = useRef<HTMLDivElement>(null);

  // Client-side sanitisation (second layer; backend already sanitises with nh3)
  const safe = DOMPurify.sanitize(html, {
    USE_PROFILES: { html: true },
    ADD_ATTR: ["loading"],
  });

  useEffect(() => {
    if (ref.current) {
      ref.current.querySelectorAll<HTMLElement>("pre code").forEach((block) => {
        hljs.highlightElement(block);
      });
    }
  }, [html]);

  return (
    <div
      ref={ref}
      className="prose"
      // The HTML is already sanitised server-side (nh3) and client-side (DOMPurify).
      // eslint-disable-next-line react/no-danger
      dangerouslySetInnerHTML={{ __html: safe }}
    />
  );
}

export default function BlogArticlePage() {
  const { slug } = useParams<{ slug: string }>();
  const navigate = useNavigate();
  const [article, setArticle] = useState<(Article & { body_html: string }) | null>(null);
  const [loading, setLoading] = useState(true);
  const [notFound, setNotFound] = useState(false);

  useEffect(() => {
    if (!slug) return;
    setLoading(true);
    setNotFound(false);

    blogApi
      .getArticle(slug)
      .then((data) => {
        setArticle(data);
        // If the slug was redirected to a canonical one, update the URL silently
        if (data.slug !== slug) {
          navigate(`/blog/${data.slug}`, { replace: true });
        }
      })
      .catch((err) => {
        if (err.status === 404) setNotFound(true);
      })
      .finally(() => setLoading(false));
  }, [slug, navigate]);

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <span className="text-text-dim text-sm animate-pulse">Loading…</span>
      </div>
    );
  }

  if (notFound || !article) {
    return (
      <div className="max-w-[720px] mx-auto px-6 py-24 text-center">
        <p className="text-text-dim text-4xl mb-4">404</p>
        <p className="text-text-secondary mb-8">Article not found.</p>
        <Link to="/blog" className="text-accent no-underline hover:underline">
          ← Back to blog
        </Link>
      </div>
    );
  }

  const pub = article.published_at
    ? new Date(article.published_at * 1000).toLocaleDateString("en-US", {
        year: "numeric",
        month: "long",
        day: "numeric",
      })
    : "";

  return (
    <div className="max-w-[720px] mx-auto px-6 py-16">
      {/* Back link */}
      <Link
        to="/blog"
        className="inline-flex items-center gap-1 text-text-dim text-sm no-underline hover:text-text-secondary mb-10 transition-colors"
      >
        ← Blog
      </Link>

      {/* Cover image */}
      {article.cover_image_url && (
        <div className="aspect-[2/1] overflow-hidden rounded-lg mb-10">
          <img
            src={article.cover_image_url}
            alt={article.title}
            className="w-full h-full object-cover"
          />
        </div>
      )}

      {/* Tags */}
      {article.tags.length > 0 && (
        <div className="flex flex-wrap gap-2 mb-4">
          {article.tags.map((t) => (
            <TagBadge key={t} tag={t} />
          ))}
        </div>
      )}

      {/* Title */}
      <h1 className="font-display text-4xl font-bold text-text-primary leading-tight mb-6">
        {article.title}
      </h1>

      {/* Byline */}
      <div className="flex items-center gap-3 text-text-dim text-sm font-mono mb-12 pb-8 border-b border-border-dim">
        <span className="text-text-secondary">{article.author}</span>
        {pub && (
          <>
            <span>·</span>
            <time dateTime={new Date(article.published_at! * 1000).toISOString()}>
              {pub}
            </time>
          </>
        )}
        <span>·</span>
        <span>{article.reading_time_minutes} min read</span>
        {article.status === "draft" && (
          <span className="ml-auto px-2 py-0.5 rounded-sm bg-bg-elevated text-accent text-[0.65rem] tracking-widest uppercase">
            Draft
          </span>
        )}
      </div>

      {/* Body */}
      {article.body_html ? (
        <ArticleBody html={article.body_html} />
      ) : (
        <p className="text-text-dim italic">No content yet.</p>
      )}

      {/* Footer */}
      <div className="mt-16 pt-8 border-t border-border-dim">
        <Link
          to="/blog"
          className="text-accent no-underline hover:underline text-sm"
        >
          ← Back to all articles
        </Link>
      </div>
    </div>
  );
}
