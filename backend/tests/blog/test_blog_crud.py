"""
CRUD lifecycle tests for the blog API.

Tests cover the data layer end-to-end through the HTTP API using a real
Postgres transaction (rolled back after each test via the db fixture in
conftest.py).
"""

import pytest
from httpx import AsyncClient


_TEST_PASSWORD = "hunter2-correct-horse"


def _csrf() -> dict:
    return {"X-Requested-With": "XMLHttpRequest"}


async def _login(client: AsyncClient) -> None:
    resp = await client.post(
        "/api/auth/login",
        json={"username": "testadmin", "password": _TEST_PASSWORD},
    )
    assert resp.status_code == 200, resp.text


async def _create_article(client: AsyncClient, title: str = "Hello World") -> dict:
    resp = await client.post(
        "/api/admin/articles", json={"title": title}, headers=_csrf()
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


# ── Create ────────────────────────────────────────────────────────────────────

async def test_create_article_returns_correct_fields(client):
    await _login(client)
    article = await _create_article(client, "My First Post")

    assert article["title"] == "My First Post"
    assert article["slug"] == "my-first-post"
    assert article["status"] == "draft"
    assert article["tags"] == []
    assert article["body"] == ""
    assert article["slug_frozen"] is False
    assert article["published_at"] is None
    assert "id" in article
    assert "created_at" in article


async def test_create_article_slugifies_title(client):
    await _login(client)
    article = await _create_article(client, "  Hello   World!  ")
    assert article["slug"] == "hello-world"


async def test_create_duplicate_title_gets_unique_slug(client):
    await _login(client)
    a1 = await _create_article(client, "Duplicate")
    a2 = await _create_article(client, "Duplicate")
    assert a1["slug"] != a2["slug"]
    assert a2["slug"] == "duplicate-2"


async def test_create_empty_title_rejected(client):
    await _login(client)
    resp = await client.post(
        "/api/admin/articles", json={"title": "   "}, headers=_csrf()
    )
    assert resp.status_code == 422


# ── Read ──────────────────────────────────────────────────────────────────────

async def test_get_article_by_id(client):
    await _login(client)
    created = await _create_article(client, "Readable")

    resp = await client.get(f"/api/admin/articles/{created['id']}")
    assert resp.status_code == 200
    assert resp.json()["title"] == "Readable"


async def test_get_article_by_slug_public(client):
    await _login(client)
    created = await _create_article(client, "Public Slug")
    # publish it first
    await client.put(
        f"/api/admin/articles/{created['id']}",
        json={"status": "published", "body": "Content here"},
        headers=_csrf(),
    )

    client.cookies.clear()
    resp = await client.get(f"/api/articles/{created['slug']}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["title"] == "Public Slug"
    assert "body_html" in data
    assert "body" not in data  # raw body stripped from public response


async def test_get_unknown_article_returns_404(client):
    resp = await client.get("/api/articles/does-not-exist")
    assert resp.status_code == 404


# ── Update ────────────────────────────────────────────────────────────────────

async def test_update_title_changes_slug_while_unfrozen(client):
    await _login(client)
    article = await _create_article(client, "Original Title")
    assert article["slug_frozen"] is False

    resp = await client.put(
        f"/api/admin/articles/{article['id']}",
        json={"title": "Updated Title"},
        headers=_csrf(),
    )
    assert resp.status_code == 200
    updated = resp.json()["article"]
    assert updated["title"] == "Updated Title"
    assert updated["slug"] == "updated-title"


async def test_publishing_freezes_slug(client):
    await _login(client)
    article = await _create_article(client, "To Publish")

    resp = await client.put(
        f"/api/admin/articles/{article['id']}",
        json={"status": "published"},
        headers=_csrf(),
    )
    assert resp.status_code == 200
    updated = resp.json()["article"]
    assert updated["status"] == "published"
    assert updated["slug_frozen"] is True
    assert updated["published_at"] is not None


async def test_publishing_frozen_slug_not_changed_by_title_update(client):
    await _login(client)
    article = await _create_article(client, "Frozen Post")

    # Publish — freezes slug
    await client.put(
        f"/api/admin/articles/{article['id']}",
        json={"status": "published"},
        headers=_csrf(),
    )

    # Update title — slug must NOT change
    resp = await client.put(
        f"/api/admin/articles/{article['id']}",
        json={"title": "Completely Different Title"},
        headers=_csrf(),
    )
    updated = resp.json()["article"]
    assert updated["slug"] == "frozen-post"


async def test_explicit_slug_change_records_redirect(client):
    await _login(client)
    article = await _create_article(client, "Old Slug Article")
    old_slug = article["slug"]

    await client.put(
        f"/api/admin/articles/{article['id']}",
        json={"slug": "new-slug"},
        headers=_csrf(),
    )

    # Old slug should now redirect to the new article
    resp = await client.get(f"/api/articles/{old_slug}")
    # Article is a draft so 404 for unauthenticated — login and retry
    await _login(client)
    resp = await client.get(f"/api/articles/{old_slug}")
    assert resp.status_code == 200
    assert resp.json()["slug"] == "new-slug"


async def test_update_tags(client):
    await _login(client)
    article = await _create_article(client)

    resp = await client.put(
        f"/api/admin/articles/{article['id']}",
        json={"tags": ["python", "fastapi"]},
        headers=_csrf(),
    )
    assert resp.json()["article"]["tags"] == ["python", "fastapi"]


async def test_update_nonexistent_article_returns_404(client):
    await _login(client)
    resp = await client.put(
        "/api/admin/articles/99999",
        json={"title": "Ghost"},
        headers=_csrf(),
    )
    assert resp.status_code == 404


# ── List ──────────────────────────────────────────────────────────────────────

async def test_public_list_only_returns_published(client):
    await _login(client)
    draft = await _create_article(client, "Draft Post")
    pub = await _create_article(client, "Published Post")

    await client.put(
        f"/api/admin/articles/{pub['id']}",
        json={"status": "published"},
        headers=_csrf(),
    )

    client.cookies.clear()
    resp = await client.get("/api/articles")
    assert resp.status_code == 200
    data = resp.json()
    slugs = [a["slug"] for a in data["articles"]]
    assert pub["slug"] in slugs
    assert draft["slug"] not in slugs


async def test_list_pagination(client):
    await _login(client)
    for i in range(5):
        a = await _create_article(client, f"Page Article {i}")
        await client.put(
            f"/api/admin/articles/{a['id']}",
            json={"status": "published"},
            headers=_csrf(),
        )

    resp = await client.get("/api/articles?page=1&per_page=2")
    data = resp.json()
    assert data["per_page"] == 2
    assert len(data["articles"]) == 2
    assert data["total"] >= 5
    assert data["total_pages"] >= 3


async def test_admin_list_includes_drafts(client):
    await _login(client)
    draft = await _create_article(client, "Hidden Draft")

    resp = await client.get("/api/admin/articles")
    slugs = [a["slug"] for a in resp.json()["articles"]]
    assert draft["slug"] in slugs


# ── Delete ────────────────────────────────────────────────────────────────────

async def test_delete_article(client):
    await _login(client)
    article = await _create_article(client, "To Delete")

    resp = await client.delete(
        f"/api/admin/articles/{article['id']}", headers=_csrf()
    )
    assert resp.status_code == 200
    assert resp.json()["ok"] is True

    resp = await client.get(f"/api/admin/articles/{article['id']}")
    assert resp.status_code == 404


async def test_delete_nonexistent_article_returns_404(client):
    await _login(client)
    resp = await client.delete("/api/admin/articles/99999", headers=_csrf())
    assert resp.status_code == 404


# ── reading_time_minutes ──────────────────────────────────────────────────────

async def test_reading_time_computed_correctly(client):
    await _login(client)
    article = await _create_article(client, "Long Post")
    # 400 words → ceil(400/200) = 2 minutes
    body = " ".join(["word"] * 400)
    resp = await client.put(
        f"/api/admin/articles/{article['id']}",
        json={"body": body, "status": "published"},
        headers=_csrf(),
    )
    updated = resp.json()["article"]
    assert updated["reading_time_minutes"] == 2
