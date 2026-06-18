"""Auth boundary tests for the blog API."""

import os
import pytest
from httpx import AsyncClient


_TEST_PASSWORD = "hunter2-correct-horse"


def _csrf_headers() -> dict:
    return {"X-Requested-With": "XMLHttpRequest"}


async def _login(client: AsyncClient) -> None:
    resp = await client.post(
        "/api/auth/login",
        json={"username": "testadmin", "password": _TEST_PASSWORD},
    )
    assert resp.status_code == 200, resp.text


# ── Auth boundary: unauthenticated requests must be rejected ──────────────────

async def test_create_article_requires_auth(client):
    resp = await client.post(
        "/api/admin/articles",
        json={"title": "Secret"},
        headers=_csrf_headers(),
    )
    assert resp.status_code == 401


async def test_update_article_requires_auth(client):
    resp = await client.put(
        "/api/admin/articles/1",
        json={"title": "Updated"},
        headers=_csrf_headers(),
    )
    assert resp.status_code == 401


async def test_delete_article_requires_auth(client):
    resp = await client.delete("/api/admin/articles/1", headers=_csrf_headers())
    assert resp.status_code == 401


async def test_media_upload_requires_auth(client):
    resp = await client.post(
        "/api/media",
        files={"file": ("x.jpg", b"\xff\xd8\xff\xe0" + b"\x00" * 100, "image/jpeg")},
        headers=_csrf_headers(),
    )
    assert resp.status_code == 401


async def test_admin_list_requires_auth(client):
    resp = await client.get("/api/admin/articles")
    assert resp.status_code == 401


# ── Login ─────────────────────────────────────────────────────────────────────

async def test_login_wrong_password_returns_generic_error(client):
    resp = await client.post(
        "/api/auth/login",
        json={"username": "testadmin", "password": "wrongpassword"},
    )
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid credentials"


async def test_login_wrong_username_returns_same_error(client):
    resp = await client.post(
        "/api/auth/login",
        json={"username": "notadmin", "password": _TEST_PASSWORD},
    )
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid credentials"


async def test_login_success_sets_httponly_cookie(client):
    resp = await client.post(
        "/api/auth/login",
        json={"username": "testadmin", "password": _TEST_PASSWORD},
    )
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
    assert "session" in client.cookies


async def test_me_endpoint_reflects_auth_state(client):
    assert (await client.get("/api/auth/me")).json() == {"authenticated": False}
    await _login(client)
    assert (await client.get("/api/auth/me")).json() == {"authenticated": True}


# ── CSRF protection ───────────────────────────────────────────────────────────

async def test_create_article_requires_csrf_header(client):
    await _login(client)
    resp = await client.post("/api/admin/articles", json={"title": "Test"})
    assert resp.status_code == 403


async def test_delete_requires_csrf_header(client):
    await _login(client)
    resp = await client.delete("/api/admin/articles/1")
    assert resp.status_code == 403


# ── Draft visibility ──────────────────────────────────────────────────────────

async def test_draft_not_visible_to_public(client):
    await _login(client)
    create = await client.post(
        "/api/admin/articles",
        json={"title": "My Draft"},
        headers=_csrf_headers(),
    )
    assert create.status_code == 201
    slug = create.json()["slug"]

    client.cookies.clear()
    resp = await client.get(f"/api/articles/{slug}")
    assert resp.status_code == 404


async def test_draft_visible_to_authenticated_admin(client):
    await _login(client)
    create = await client.post(
        "/api/admin/articles",
        json={"title": "Admin-only Draft"},
        headers=_csrf_headers(),
    )
    assert create.status_code == 201
    slug = create.json()["slug"]

    resp = await client.get(f"/api/articles/{slug}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "draft"


# ── Upload validation ─────────────────────────────────────────────────────────

async def test_non_image_file_rejected(client):
    await _login(client)
    resp = await client.post(
        "/api/media",
        files={"file": ("resume.pdf", b"%PDF-1.4 fake", "application/pdf")},
        headers=_csrf_headers(),
    )
    assert resp.status_code == 422


async def test_oversized_upload_rejected(client, monkeypatch):
    monkeypatch.setenv("MAX_UPLOAD_MB", "1")
    await _login(client)
    big_data = b"\xff\xd8\xff" + b"\x00" * (2 * 1024 * 1024)
    resp = await client.post(
        "/api/media",
        files={"file": ("big.jpg", big_data, "image/jpeg")},
        headers=_csrf_headers(),
    )
    assert resp.status_code == 413


# ── Startup config validation ─────────────────────────────────────────────────

def test_missing_auth_secret_raises():
    from app.blog.auth import validate_auth_config

    saved = {k: os.environ.pop(k, None) for k in ("AUTH_SECRET", "ADMIN_USERNAME", "ADMIN_PASSWORD_HASH")}
    os.environ["ADMIN_USERNAME"] = "admin"
    os.environ["ADMIN_PASSWORD_HASH"] = "hash"

    with pytest.raises(ValueError, match="AUTH_SECRET"):
        validate_auth_config()

    for k, v in saved.items():
        if v is not None:
            os.environ[k] = v
        else:
            os.environ.pop(k, None)
