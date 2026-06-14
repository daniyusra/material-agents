"""
SQLite persistence for the blog: articles, slug redirects, media records.

Mirrors the pattern from app/whatsapp/db.py — raw sqlite3, WAL mode,
@contextmanager connection helper, no ORM, tables created on startup.

BLOG_DB_PATH env var (default: backend/blog.db) is read at call time so
tests can monkeypatch it without import-time caching issues.
"""

import json
import math
import os
import re
import sqlite3
import time
import unicodedata
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional


def _blog_db_path() -> Path:
    default = str(Path(__file__).parent.parent.parent / "blog.db")
    return Path(os.getenv("BLOG_DB_PATH", default))


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_blog_db_path()), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def _db() -> Generator[sqlite3.Connection, None, None]:
    conn = _connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with _db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS blog_articles (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                title            TEXT NOT NULL,
                slug             TEXT UNIQUE NOT NULL,
                body             TEXT NOT NULL DEFAULT '',
                excerpt          TEXT NOT NULL DEFAULT '',
                meta_description TEXT,
                cover_image_url  TEXT,
                og_image_url     TEXT,
                tags             TEXT NOT NULL DEFAULT '[]',
                status           TEXT NOT NULL DEFAULT 'draft'
                                     CHECK(status IN ('draft', 'published')),
                published_at     INTEGER,
                created_at       INTEGER NOT NULL,
                updated_at       INTEGER NOT NULL,
                author           TEXT NOT NULL DEFAULT 'Admin',
                slug_frozen      INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS blog_slug_redirects (
                old_slug    TEXT PRIMARY KEY,
                article_id  INTEGER NOT NULL,
                created_at  INTEGER NOT NULL,
                FOREIGN KEY (article_id) REFERENCES blog_articles(id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS blog_media (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id  INTEGER,
                filename    TEXT NOT NULL,
                url         TEXT NOT NULL,
                alt_text    TEXT,
                size_bytes  INTEGER NOT NULL,
                created_at  INTEGER NOT NULL,
                FOREIGN KEY (article_id) REFERENCES blog_articles(id)
                    ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_blog_articles_slug
                ON blog_articles(slug);
            CREATE INDEX IF NOT EXISTS idx_blog_articles_status_pub
                ON blog_articles(status, published_at);
            CREATE INDEX IF NOT EXISTS idx_blog_media_article
                ON blog_media(article_id);
        """)


# ── Slug utilities ────────────────────────────────────────────────────────────

def _slugify(title: str) -> str:
    normalized = unicodedata.normalize("NFKD", title)
    ascii_str = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_str.lower()).strip("-")
    return (slug or "article")[:80]


def _unique_slug(title: str, exclude_id: Optional[int] = None) -> str:
    base = _slugify(title)
    candidate = base
    n = 2
    while True:
        with _db() as conn:
            row = conn.execute(
                "SELECT id FROM blog_articles WHERE slug = ?", (candidate,)
            ).fetchone()
        if row is None or (exclude_id is not None and row["id"] == exclude_id):
            return candidate
        candidate = f"{base}-{n}"
        n += 1


# ── Row helpers ───────────────────────────────────────────────────────────────

def _row_to_article(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["tags"] = json.loads(d.get("tags") or "[]")
    word_count = len((d.get("body") or "").split())
    d["reading_time_minutes"] = max(1, math.ceil(word_count / 200))
    return d


# ── Articles — write ──────────────────────────────────────────────────────────

def create_article(title: str) -> dict:
    now = int(time.time())
    slug = _unique_slug(title)
    with _db() as conn:
        cur = conn.execute(
            """INSERT INTO blog_articles
               (title, slug, body, excerpt, tags, status, created_at, updated_at, author)
               VALUES (?, ?, '', '', '[]', 'draft', ?, ?, 'Admin')""",
            (title, slug, now, now),
        )
        row_id: int = cur.lastrowid  # type: ignore[assignment]
    return get_article_by_id(row_id)  # type: ignore[return-value]


def update_article(article_id: int, fields: dict) -> Optional[dict]:
    now = int(time.time())
    article = get_article_by_id(article_id)
    if article is None:
        return None

    update: dict = {}

    if "title" in fields:
        update["title"] = fields["title"]
        # Auto-update slug from title only while slug is still unfrozen
        if not article["slug_frozen"] and "slug" not in fields:
            new_slug = _unique_slug(fields["title"], exclude_id=article_id)
            if new_slug != article["slug"]:
                update["slug"] = new_slug

    # Explicit slug override (admin changes slug manually)
    if "slug" in fields and fields["slug"] != article["slug"]:
        _record_redirect(article["slug"], article_id)
        update["slug"] = fields["slug"]

    # Publishing: freeze slug, set published_at once
    if fields.get("status") == "published":
        update["status"] = "published"
        if not article["slug_frozen"]:
            update["slug_frozen"] = 1
        if not article.get("published_at"):
            update["published_at"] = fields.get("published_at") or now

    elif "status" in fields:
        update["status"] = fields["status"]

    for key in ("body", "excerpt", "meta_description", "cover_image_url", "og_image_url", "author"):
        if key in fields:
            update[key] = fields[key]

    if "tags" in fields:
        tags = fields["tags"]
        update["tags"] = json.dumps(tags if isinstance(tags, list) else [])

    update["updated_at"] = now

    if update:
        set_clause = ", ".join(f"{k} = ?" for k in update)
        with _db() as conn:
            conn.execute(
                f"UPDATE blog_articles SET {set_clause} WHERE id = ?",
                list(update.values()) + [article_id],
            )

    return get_article_by_id(article_id)


def delete_article(article_id: int) -> bool:
    with _db() as conn:
        cur = conn.execute("DELETE FROM blog_articles WHERE id = ?", (article_id,))
    return cur.rowcount > 0


def _record_redirect(old_slug: str, article_id: int) -> None:
    now = int(time.time())
    with _db() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO blog_slug_redirects
               (old_slug, article_id, created_at) VALUES (?, ?, ?)""",
            (old_slug, article_id, now),
        )


# ── Articles — read ───────────────────────────────────────────────────────────

def get_article_by_id(article_id: int) -> Optional[dict]:
    with _db() as conn:
        row = conn.execute(
            "SELECT * FROM blog_articles WHERE id = ?", (article_id,)
        ).fetchone()
    return _row_to_article(row) if row else None


def get_article_by_slug(slug: str) -> Optional[dict]:
    with _db() as conn:
        row = conn.execute(
            "SELECT * FROM blog_articles WHERE slug = ?", (slug,)
        ).fetchone()
    return _row_to_article(row) if row else None


def get_redirect_for_slug(old_slug: str) -> Optional[dict]:
    with _db() as conn:
        row = conn.execute(
            "SELECT * FROM blog_slug_redirects WHERE old_slug = ?", (old_slug,)
        ).fetchone()
    return dict(row) if row else None


def list_published_articles(page: int = 1, per_page: int = 10) -> dict:
    offset = (page - 1) * per_page
    with _db() as conn:
        total: int = conn.execute(
            "SELECT COUNT(*) FROM blog_articles WHERE status = 'published'"
        ).fetchone()[0]
        rows = conn.execute(
            """SELECT id, title, slug, excerpt, cover_image_url, og_image_url,
                      tags, published_at, author, body, status
               FROM blog_articles WHERE status = 'published'
               ORDER BY published_at DESC LIMIT ? OFFSET ?""",
            (per_page, offset),
        ).fetchall()

    articles = [_row_to_article(r) for r in rows]
    for a in articles:
        del a["body"]  # list endpoint never exposes raw body

    return {
        "articles": articles,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": math.ceil(total / per_page) if total else 0,
    }


def list_all_articles() -> list[dict]:
    """All articles including drafts — admin only."""
    with _db() as conn:
        rows = conn.execute(
            """SELECT id, title, slug, excerpt, cover_image_url, tags,
                      status, published_at, author, body, created_at, updated_at,
                      slug_frozen
               FROM blog_articles ORDER BY updated_at DESC"""
        ).fetchall()
    articles = [_row_to_article(r) for r in rows]
    for a in articles:
        del a["body"]
    return articles


def get_published_articles_for_feed() -> list[dict]:
    """Lightweight fetch for sitemap/RSS — returns only essential fields."""
    with _db() as conn:
        rows = conn.execute(
            """SELECT slug, title, excerpt, published_at, updated_at
               FROM blog_articles WHERE status = 'published'
               ORDER BY published_at DESC LIMIT 100"""
        ).fetchall()
    return [dict(r) for r in rows]


# ── Media ─────────────────────────────────────────────────────────────────────

def record_media(
    filename: str,
    url: str,
    size_bytes: int,
    alt_text: Optional[str] = None,
) -> dict:
    now = int(time.time())
    with _db() as conn:
        cur = conn.execute(
            """INSERT INTO blog_media (filename, url, alt_text, size_bytes, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (filename, url, alt_text, size_bytes, now),
        )
        return {"id": cur.lastrowid, "url": url, "alt_text": alt_text}


def associate_media(article_id: int, urls: list[str]) -> None:
    """Link previously-unowned media records to an article by URL."""
    if not urls:
        return
    placeholders = ",".join("?" * len(urls))
    with _db() as conn:
        conn.execute(
            f"""UPDATE blog_media SET article_id = ?
                WHERE url IN ({placeholders}) AND article_id IS NULL""",
            [article_id, *urls],
        )


def get_media_for_article(article_id: int) -> list[dict]:
    with _db() as conn:
        rows = conn.execute(
            "SELECT * FROM blog_media WHERE article_id = ?", (article_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_orphaned_media(older_than_seconds: int = 86400) -> list[dict]:
    """Media records with no article, older than the given age (default 24h)."""
    cutoff = int(time.time()) - older_than_seconds
    with _db() as conn:
        rows = conn.execute(
            "SELECT * FROM blog_media WHERE article_id IS NULL AND created_at < ?",
            (cutoff,),
        ).fetchall()
    return [dict(r) for r in rows]


def delete_media_record(media_id: int) -> None:
    with _db() as conn:
        conn.execute("DELETE FROM blog_media WHERE id = ?", (media_id,))
