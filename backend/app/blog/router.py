"""
Blog API routes — public, admin-gated, auth, and feed endpoints.

Security model:
  - All /api/admin/* and /api/media require a valid session cookie (HTTP 401
    without it). State-changing endpoints also check the X-Requested-With
    header as a CSRF guard. The frontend route guard is UX only; this server
    enforces every boundary.
  - Draft articles 404 for unauthenticated requests.
  - Upload: allowlisted MIME types, magic-byte validation, size cap, random
    filename, re-encoded via Pillow to strip metadata.
"""

import logging
import os
import time
import uuid
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Response, UploadFile, Form
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from . import crud as blog_db
from .auth import (
    AdminAuth,
    AdminAuthWithCsrf,
    blog_limiter,
    handle_login,
    handle_logout,
    handle_me,
    LoginRequest,
    optional_admin,
)
from .database import get_db
from .image_processor import detect_image_type, ALLOWED_MIME_TYPES, process_image
from .markdown_renderer import (
    extract_media_urls,
    missing_alt_images,
    render_markdown,
)
from .storage_backend import get_storage_backend

log = logging.getLogger(__name__)

router = APIRouter()

_SITE_BASE_URL = os.getenv("SITE_BASE_URL", "").rstrip("/")
_MEDIA_BASE_URL = os.getenv("MEDIA_BASE_URL", "").rstrip("/")


def _absolute_media_url(path: str) -> str:
    """Prefix a relative /media/... path with MEDIA_BASE_URL if configured."""
    if _MEDIA_BASE_URL and path.startswith("/"):
        return f"{_MEDIA_BASE_URL}{path}"
    return path


def _max_upload_bytes() -> int:
    return int(os.getenv("MAX_UPLOAD_MB", "10")) * 1024 * 1024


# ── Auth ──────────────────────────────────────────────────────────────────────

@router.post("/api/auth/login")
@blog_limiter.limit(os.getenv("RATELIMIT_LOGIN", "5/minute"))
async def login(request: Request, body: LoginRequest, response: Response):
    return await handle_login(request, body, response)


@router.post("/api/auth/logout", dependencies=AdminAuthWithCsrf)
async def logout(response: Response):
    return handle_logout(response)


@router.get("/api/auth/me")
async def me(session_check=Depends(handle_me)):
    return session_check


# ── Public article endpoints ───────────────────────────────────────────────────

@router.get("/api/articles")
async def list_articles(
    page: int = 1,
    per_page: int = 10,
    db: AsyncSession = Depends(get_db),
):
    per_page = min(per_page, 50)
    return await blog_db.list_published_articles(db, page=page, per_page=per_page)


@router.get("/api/articles/{slug}")
async def get_article(
    slug: str,
    is_admin: bool = Depends(optional_admin),
    db: AsyncSession = Depends(get_db),
):
    article = await blog_db.get_article_by_slug(db, slug)

    if article is None:
        redirect = await blog_db.get_redirect_for_slug(db, slug)
        if redirect:
            target = await blog_db.get_article_by_id(db, redirect["article_id"])
            if target and (target["status"] == "published" or is_admin):
                article = target
            else:
                raise HTTPException(status_code=404, detail="Article not found")
        else:
            raise HTTPException(status_code=404, detail="Article not found")

    if article["status"] == "draft" and not is_admin:
        raise HTTPException(status_code=404, detail="Article not found")

    result = dict(article)
    result["body_html"] = render_markdown(article.get("body", ""))
    if not is_admin:
        result.pop("body", None)

    return result


# ── Admin article endpoints ────────────────────────────────────────────────────

class ArticleCreateRequest(BaseModel):
    title: str


class ArticleUpdateRequest(BaseModel):
    title: Optional[str] = None
    slug: Optional[str] = None
    body: Optional[str] = None
    excerpt: Optional[str] = None
    meta_description: Optional[str] = None
    cover_image_url: Optional[str] = None
    og_image_url: Optional[str] = None
    tags: Optional[list[str]] = None
    status: Optional[Literal["draft", "published"]] = None


@router.get("/api/admin/articles", dependencies=AdminAuth)
async def admin_list_articles(db: AsyncSession = Depends(get_db)):
    return {"articles": await blog_db.list_all_articles(db)}


@router.get("/api/admin/articles/{article_id}", dependencies=AdminAuth)
async def admin_get_article(article_id: int, db: AsyncSession = Depends(get_db)):
    article = await blog_db.get_article_by_id(db, article_id)
    if article is None:
        raise HTTPException(status_code=404, detail="Article not found")
    return article


@router.post("/api/admin/articles", dependencies=AdminAuthWithCsrf, status_code=201)
async def admin_create_article(
    body: ArticleCreateRequest,
    db: AsyncSession = Depends(get_db),
):
    if not body.title.strip():
        raise HTTPException(status_code=422, detail="Title cannot be empty")
    article = await blog_db.create_article(db, body.title.strip())
    log.info("blog_article_created", extra={"article_id": article["id"], "slug": article["slug"]})
    return article


@router.put("/api/admin/articles/{article_id}", dependencies=AdminAuthWithCsrf)
async def admin_update_article(
    article_id: int,
    body: ArticleUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    existing = await blog_db.get_article_by_id(db, article_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Article not found")

    fields = body.model_dump(exclude_none=True)

    warnings: list[str] = []
    if fields.get("status") == "published" and "body" in fields:
        missing = missing_alt_images(fields["body"])
        if missing:
            warnings.append(f"{len(missing)} image(s) have missing alt text")

    article = await blog_db.update_article(db, article_id, fields)
    if article is None:
        raise HTTPException(status_code=404, detail="Article not found")

    if "body" in fields:
        urls = extract_media_urls(fields["body"])
        await blog_db.associate_media(db, article_id, urls)

    log.info(
        "blog_article_updated",
        extra={"article_id": article_id, "status": article.get("status")},
    )
    return {"article": article, "warnings": warnings}


@router.delete("/api/admin/articles/{article_id}", dependencies=AdminAuthWithCsrf)
async def admin_delete_article(
    article_id: int,
    db: AsyncSession = Depends(get_db),
):
    existing = await blog_db.get_article_by_id(db, article_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Article not found")

    storage = get_storage_backend()
    for media in await blog_db.get_media_for_article(db, article_id):
        try:
            storage.delete(media["url"])
        except Exception:
            log.warning("blog_media_delete_failed", extra={"url": media.get("url")})

    deleted = await blog_db.delete_article(db, article_id)
    log.info("blog_article_deleted", extra={"article_id": article_id})
    return {"ok": deleted}


# ── Media upload ──────────────────────────────────────────────────────────────

@router.post("/api/media", dependencies=AdminAuthWithCsrf)
async def upload_media(
    file: UploadFile,
    alt_text: str = Form(default=""),
    db: AsyncSession = Depends(get_db),
):
    data = await file.read()

    if len(data) > _max_upload_bytes():
        raise HTTPException(
            status_code=413,
            detail=f"File too large (max {os.getenv('MAX_UPLOAD_MB', '10')} MB)",
        )

    detected = detect_image_type(data)
    if detected is None or detected not in ALLOWED_MIME_TYPES:
        raise HTTPException(
            status_code=422,
            detail="Unsupported file type. Allowed: JPEG, PNG, GIF, WebP",
        )

    try:
        result = process_image(data)
    except Exception as exc:
        log.warning("blog_image_process_failed", extra={"error": str(exc)})
        raise HTTPException(status_code=422, detail="Could not process image")

    stem = uuid.uuid4().hex
    ext = result["ext"]
    main_filename = f"{stem}.{ext}"
    thumb_filename = f"{stem}-thumb.{ext}"

    storage = get_storage_backend()
    main_url = storage.save(result["main"], main_filename, result["main_type"])
    storage.save(result["thumb"], thumb_filename, result["thumb_type"])

    await blog_db.record_media(db, main_filename, main_url, len(result["main"]), alt_text or None)

    log.info(
        "blog_media_uploaded",
        extra={"media_filename": main_filename, "size": len(result["main"])},
    )
    return {
        "url": _absolute_media_url(main_url),
        "thumb_url": _absolute_media_url(f"/media/{thumb_filename}"),
        "width": result["width"],
        "height": result["height"],
        "alt_text": alt_text,
    }


# ── Feeds & discoverability ───────────────────────────────────────────────────

def _format_rfc822(ts: int) -> str:
    import email.utils
    return email.utils.formatdate(ts, usegmt=True)


def _format_iso(ts: int) -> str:
    import datetime
    return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).strftime("%Y-%m-%d")


@router.get("/sitemap.xml", response_class=PlainTextResponse)
async def sitemap(db: AsyncSession = Depends(get_db)):
    articles = await blog_db.get_published_articles_for_feed(db)
    base = _SITE_BASE_URL or "https://example.com"

    urls = [
        f"""  <url>
    <loc>{base}/blog</loc>
    <changefreq>weekly</changefreq>
    <priority>0.8</priority>
  </url>"""
    ]
    for a in articles:
        lastmod = _format_iso(a["updated_at"] or a["published_at"])
        urls.append(
            f"""  <url>
    <loc>{base}/blog/{a['slug']}</loc>
    <lastmod>{lastmod}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.7</priority>
  </url>"""
        )

    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(urls)
        + "\n</urlset>"
    )
    return PlainTextResponse(xml, media_type="application/xml")


@router.get("/feed.xml", response_class=PlainTextResponse)
async def rss_feed(db: AsyncSession = Depends(get_db)):
    articles = await blog_db.get_published_articles_for_feed(db)
    base = _SITE_BASE_URL or "https://example.com"
    now_rfc = _format_rfc822(int(time.time()))

    items = []
    for a in articles:
        pub = _format_rfc822(a["published_at"]) if a.get("published_at") else now_rfc
        link = f"{base}/blog/{a['slug']}"
        excerpt = (a.get("excerpt") or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        title = (a.get("title") or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        items.append(
            f"""  <item>
    <title>{title}</title>
    <link>{link}</link>
    <guid isPermaLink="true">{link}</guid>
    <pubDate>{pub}</pubDate>
    <description>{excerpt}</description>
  </item>"""
        )

    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">\n'
        "  <channel>\n"
        f"    <title>Blog</title>\n"
        f"    <link>{base}/blog</link>\n"
        f"    <description>Latest articles</description>\n"
        f"    <lastBuildDate>{now_rfc}</lastBuildDate>\n"
        f'    <atom:link href="{base}/feed.xml" rel="self" type="application/rss+xml"/>\n'
        + "\n".join(items)
        + "\n  </channel>\n</rss>"
    )
    return PlainTextResponse(xml, media_type="application/rss+xml")


@router.get("/robots.txt", response_class=PlainTextResponse)
async def robots():
    base = _SITE_BASE_URL or "https://example.com"
    txt = f"User-agent: *\nAllow: /\nDisallow: /admin\nSitemap: {base}/sitemap.xml\n"
    return PlainTextResponse(txt, media_type="text/plain")
