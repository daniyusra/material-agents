"""
Auth boundary tests for the blog API.

Tests are run against the blog router directly (not app.main) so env vars
can be monkeypatched safely without import-time caching issues.
"""

import io

import bcrypt
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

_TEST_PASSWORD = "hunter2-correct-horse"


@pytest.fixture
def client(monkeypatch, tmp_path):
    pw_hash = bcrypt.hashpw(_TEST_PASSWORD.encode(), bcrypt.gensalt()).decode()

    monkeypatch.setenv("AUTH_SECRET", "x" * 32)
    monkeypatch.setenv("ADMIN_USERNAME", "testadmin")
    monkeypatch.setenv("ADMIN_PASSWORD_HASH", pw_hash)
    monkeypatch.setenv("COOKIE_SECURE", "false")
    monkeypatch.setenv("BLOG_DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("MEDIA_DIR", str(tmp_path / "media"))
    monkeypatch.setenv("RATELIMIT_LOGIN", "100/minute")  # avoid hitting limit in tests

    # Clear the storage backend lru_cache so MEDIA_DIR is re-evaluated
    from app.blog.storage_backend import get_storage_backend
    get_storage_backend.cache_clear()

    # Disable rate limiting on the blog limiter to keep tests fast
    from app.blog.auth import blog_limiter
    blog_limiter._enabled = False

    from app.blog import db as blog_db
    from app.blog.auth import validate_auth_config
    from app.blog.router import router as blog_router

    validate_auth_config()  # confirm the test config is valid
    blog_db.init_db()

    test_app = FastAPI()
    test_app.include_router(blog_router)
    test_app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

    with TestClient(test_app, raise_server_exceptions=True) as c:
        yield c

    blog_limiter._enabled = True
    get_storage_backend.cache_clear()


# ── Helper ────────────────────────────────────────────────────────────────────

def _login(client: TestClient) -> None:
    resp = client.post(
        "/api/auth/login",
        json={"username": "testadmin", "password": _TEST_PASSWORD},
    )
    assert resp.status_code == 200, resp.text


def _csrf_headers() -> dict:
    return {"X-Requested-With": "XMLHttpRequest"}


# ── Auth boundary: unauthenticated requests must be rejected ──────────────────

def test_create_article_requires_auth(client):
    resp = client.post(
        "/api/admin/articles",
        json={"title": "Secret"},
        headers=_csrf_headers(),
    )
    assert resp.status_code == 401


def test_update_article_requires_auth(client):
    resp = client.put(
        "/api/admin/articles/1",
        json={"title": "Updated"},
        headers=_csrf_headers(),
    )
    assert resp.status_code == 401


def test_delete_article_requires_auth(client):
    resp = client.delete("/api/admin/articles/1", headers=_csrf_headers())
    assert resp.status_code == 401


def test_media_upload_requires_auth(client):
    resp = client.post(
        "/api/media",
        files={"file": ("x.jpg", b"\xff\xd8\xff\xe0" + b"\x00" * 100, "image/jpeg")},
        headers=_csrf_headers(),
    )
    assert resp.status_code == 401


def test_admin_list_requires_auth(client):
    resp = client.get("/api/admin/articles")
    assert resp.status_code == 401


# ── Login ─────────────────────────────────────────────────────────────────────

def test_login_wrong_password_returns_generic_error(client):
    resp = client.post(
        "/api/auth/login",
        json={"username": "testadmin", "password": "wrongpassword"},
    )
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid credentials"


def test_login_wrong_username_returns_same_error(client):
    resp = client.post(
        "/api/auth/login",
        json={"username": "notadmin", "password": _TEST_PASSWORD},
    )
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid credentials"


def test_login_success_sets_httponly_cookie(client):
    resp = client.post(
        "/api/auth/login",
        json={"username": "testadmin", "password": _TEST_PASSWORD},
    )
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
    assert "session" in client.cookies, "Session cookie should be set after login"


def test_me_endpoint_reflects_auth_state(client):
    assert client.get("/api/auth/me").json() == {"authenticated": False}
    _login(client)
    assert client.get("/api/auth/me").json() == {"authenticated": True}


# ── CSRF protection ───────────────────────────────────────────────────────────

def test_create_article_requires_csrf_header(client):
    _login(client)
    resp = client.post(
        "/api/admin/articles",
        json={"title": "Test"},
        # No X-Requested-With header
    )
    assert resp.status_code == 403


def test_delete_requires_csrf_header(client):
    _login(client)
    resp = client.delete("/api/admin/articles/1")  # No CSRF header
    assert resp.status_code == 403


# ── Draft visibility ──────────────────────────────────────────────────────────

def test_draft_not_visible_to_public(client):
    _login(client)
    create = client.post(
        "/api/admin/articles",
        json={"title": "My Draft"},
        headers=_csrf_headers(),
    )
    assert create.status_code == 201
    slug = create.json()["slug"]
    assert slug  # sanity

    # Clear the session cookie → unauthenticated
    saved = dict(client.cookies)
    client.cookies.clear()

    resp = client.get(f"/api/articles/{slug}")
    assert resp.status_code == 404, "Drafts must be hidden from unauthenticated users"

    # Restore cookies
    for k, v in saved.items():
        client.cookies.set(k, v)


def test_draft_visible_to_authenticated_admin(client):
    _login(client)
    create = client.post(
        "/api/admin/articles",
        json={"title": "Admin-only Draft"},
        headers=_csrf_headers(),
    )
    assert create.status_code == 201
    slug = create.json()["slug"]

    resp = client.get(f"/api/articles/{slug}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "draft"


# ── Upload validation ─────────────────────────────────────────────────────────

def test_non_image_file_rejected(client):
    _login(client)
    resp = client.post(
        "/api/media",
        files={"file": ("resume.pdf", b"%PDF-1.4 fake", "application/pdf")},
        headers=_csrf_headers(),
    )
    assert resp.status_code == 422


def test_oversized_upload_rejected(client, monkeypatch):
    monkeypatch.setenv("MAX_UPLOAD_MB", "1")
    _login(client)
    big_data = b"\xff\xd8\xff" + b"\x00" * (2 * 1024 * 1024)  # 2 MB JPEG-header fake
    resp = client.post(
        "/api/media",
        files={"file": ("big.jpg", big_data, "image/jpeg")},
        headers=_csrf_headers(),
    )
    assert resp.status_code == 413


# ── Startup config validation ─────────────────────────────────────────────────

def test_missing_auth_secret_raises():
    from app.blog.auth import validate_auth_config
    import os

    saved = {k: os.environ.pop(k, None) for k in ("AUTH_SECRET", "ADMIN_USERNAME", "ADMIN_PASSWORD_HASH")}
    os.environ["ADMIN_USERNAME"] = "admin"
    os.environ["ADMIN_PASSWORD_HASH"] = "hash"
    # AUTH_SECRET absent → should raise

    with pytest.raises(ValueError, match="AUTH_SECRET"):
        validate_auth_config()

    # Restore
    for k, v in saved.items():
        if v is not None:
            os.environ[k] = v
        else:
            os.environ.pop(k, None)
