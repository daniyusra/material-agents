"""Async data-access functions for the blog — replaces db.py.

All public functions are async and accept an AsyncSession.
The caller (router) is responsible for opening the session via get_db().
"""

import math
import re
import time
import unicodedata
from typing import Optional

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Article, Media, SlugRedirect


# ── Slug utilities ────────────────────────────────────────────────────────────

def _slugify(title: str) -> str:
    normalized = unicodedata.normalize("NFKD", title)
    ascii_str = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_str.lower()).strip("-")
    return (slug or "article")[:80]


async def _unique_slug(
    db: AsyncSession, title: str, exclude_id: Optional[int] = None
) -> str:
    base = _slugify(title)
    candidate = base
    n = 2
    while True:
        stmt = select(Article.id).where(Article.slug == candidate)
        row = (await db.execute(stmt)).first()
        if row is None or (exclude_id is not None and row[0] == exclude_id):
            return candidate
        candidate = f"{base}-{n}"
        n += 1


# ── Row serialisation ─────────────────────────────────────────────────────────

def _article_to_dict(article: Article) -> dict:
    d = {
        "id": article.id,
        "title": article.title,
        "slug": article.slug,
        "body": article.body,
        "excerpt": article.excerpt,
        "meta_description": article.meta_description,
        "cover_image_url": article.cover_image_url,
        "og_image_url": article.og_image_url,
        "tags": article.tags or [],
        "status": article.status,
        "published_at": article.published_at,
        "created_at": article.created_at,
        "updated_at": article.updated_at,
        "author": article.author,
        "slug_frozen": article.slug_frozen,
    }
    word_count = len((article.body or "").split())
    d["reading_time_minutes"] = max(1, math.ceil(word_count / 200))
    return d


# ── Articles — write ──────────────────────────────────────────────────────────

async def create_article(db: AsyncSession, title: str) -> dict:
    now = int(time.time())
    slug = await _unique_slug(db, title)
    article = Article(
        title=title,
        slug=slug,
        body="",
        excerpt="",
        tags=[],
        status="draft",
        created_at=now,
        updated_at=now,
        author="Admin",
        slug_frozen=False,
    )
    db.add(article)
    await db.flush()
    await db.refresh(article)
    return _article_to_dict(article)


async def update_article(
    db: AsyncSession, article_id: int, fields: dict
) -> Optional[dict]:
    article = await _get_article_orm(db, article_id)
    if article is None:
        return None

    now = int(time.time())

    if "title" in fields:
        article.title = fields["title"]
        if not article.slug_frozen and "slug" not in fields:
            new_slug = await _unique_slug(db, fields["title"], exclude_id=article_id)
            if new_slug != article.slug:
                article.slug = new_slug

    if "slug" in fields and fields["slug"] != article.slug:
        await _record_redirect(db, article.slug, article_id)
        article.slug = fields["slug"]

    if fields.get("status") == "published":
        article.status = "published"
        if not article.slug_frozen:
            article.slug_frozen = True
        if not article.published_at:
            article.published_at = fields.get("published_at") or now
    elif "status" in fields:
        article.status = fields["status"]

    for key in ("body", "excerpt", "meta_description", "cover_image_url", "og_image_url", "author"):
        if key in fields:
            setattr(article, key, fields[key])

    if "tags" in fields:
        tags = fields["tags"]
        article.tags = tags if isinstance(tags, list) else []

    article.updated_at = now
    await db.flush()
    await db.refresh(article)
    return _article_to_dict(article)


async def delete_article(db: AsyncSession, article_id: int) -> bool:
    result = await db.execute(
        delete(Article).where(Article.id == article_id)
    )
    return result.rowcount > 0


async def _record_redirect(db: AsyncSession, old_slug: str, article_id: int) -> None:
    now = int(time.time())
    existing = await db.get(SlugRedirect, old_slug)
    if existing:
        existing.article_id = article_id
        existing.created_at = now
    else:
        db.add(SlugRedirect(old_slug=old_slug, article_id=article_id, created_at=now))


# ── Articles — read ───────────────────────────────────────────────────────────

async def _get_article_orm(db: AsyncSession, article_id: int) -> Optional[Article]:
    return await db.get(Article, article_id)


async def get_article_by_id(db: AsyncSession, article_id: int) -> Optional[dict]:
    article = await _get_article_orm(db, article_id)
    return _article_to_dict(article) if article else None


async def get_article_by_slug(db: AsyncSession, slug: str) -> Optional[dict]:
    result = await db.execute(select(Article).where(Article.slug == slug))
    article = result.scalar_one_or_none()
    return _article_to_dict(article) if article else None


async def get_redirect_for_slug(db: AsyncSession, old_slug: str) -> Optional[dict]:
    redirect = await db.get(SlugRedirect, old_slug)
    if redirect is None:
        return None
    return {"old_slug": redirect.old_slug, "article_id": redirect.article_id, "created_at": redirect.created_at}


async def list_published_articles(
    db: AsyncSession, page: int = 1, per_page: int = 10
) -> dict:
    offset = (page - 1) * per_page

    total_result = await db.execute(
        select(func.count()).select_from(Article).where(Article.status == "published")
    )
    total: int = total_result.scalar_one()

    rows_result = await db.execute(
        select(Article)
        .where(Article.status == "published")
        .order_by(Article.published_at.desc())
        .limit(per_page)
        .offset(offset)
    )
    articles = [_article_to_dict(a) for a in rows_result.scalars()]
    for a in articles:
        del a["body"]

    return {
        "articles": articles,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": math.ceil(total / per_page) if total else 0,
    }


async def list_all_articles(db: AsyncSession) -> list[dict]:
    result = await db.execute(select(Article).order_by(Article.updated_at.desc()))
    articles = [_article_to_dict(a) for a in result.scalars()]
    for a in articles:
        del a["body"]
    return articles


async def get_published_articles_for_feed(db: AsyncSession) -> list[dict]:
    result = await db.execute(
        select(
            Article.slug,
            Article.title,
            Article.excerpt,
            Article.published_at,
            Article.updated_at,
        )
        .where(Article.status == "published")
        .order_by(Article.published_at.desc())
        .limit(100)
    )
    return [row._asdict() for row in result]


# ── Media ─────────────────────────────────────────────────────────────────────

async def record_media(
    db: AsyncSession,
    filename: str,
    url: str,
    size_bytes: int,
    alt_text: Optional[str] = None,
) -> dict:
    now = int(time.time())
    media = Media(filename=filename, url=url, alt_text=alt_text, size_bytes=size_bytes, created_at=now)
    db.add(media)
    await db.flush()
    return {"id": media.id, "url": media.url, "alt_text": media.alt_text}


async def associate_media(db: AsyncSession, article_id: int, urls: list[str]) -> None:
    if not urls:
        return
    await db.execute(
        update(Media)
        .where(Media.url.in_(urls), Media.article_id.is_(None))
        .values(article_id=article_id)
    )


async def get_media_for_article(db: AsyncSession, article_id: int) -> list[dict]:
    result = await db.execute(select(Media).where(Media.article_id == article_id))
    return [
        {
            "id": m.id,
            "article_id": m.article_id,
            "filename": m.filename,
            "url": m.url,
            "alt_text": m.alt_text,
            "size_bytes": m.size_bytes,
            "created_at": m.created_at,
        }
        for m in result.scalars()
    ]


async def get_orphaned_media(db: AsyncSession, older_than_seconds: int = 86400) -> list[dict]:
    cutoff = int(time.time()) - older_than_seconds
    result = await db.execute(
        select(Media).where(Media.article_id.is_(None), Media.created_at < cutoff)
    )
    return [
        {"id": m.id, "url": m.url, "filename": m.filename}
        for m in result.scalars()
    ]


async def delete_media_record(db: AsyncSession, media_id: int) -> None:
    await db.execute(delete(Media).where(Media.id == media_id))
