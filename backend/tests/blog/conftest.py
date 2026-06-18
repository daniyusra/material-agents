"""
Shared fixtures for blog tests.

Requires a running PostgreSQL instance pointed to by TEST_DATABASE_URL.
Example:
    TEST_DATABASE_URL=postgresql://material:material@localhost:5432/material_agents

Tables are created once at session start and dropped at session end.
Each test gets a fresh session; a session-scoped autouse fixture truncates all
blog tables before every test so data never leaks between tests.
"""

import os

os.environ.setdefault("RATELIMIT_ENABLED", "false")

import bcrypt
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import AsyncClient, ASGITransport
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.blog.models import Base
from app.blog.router import router as blog_router
from app.blog.auth import blog_limiter, validate_auth_config
from app.blog.database import get_db
from app.blog.storage_backend import get_storage_backend

_TEST_PASSWORD = "hunter2-correct-horse"

_RAW_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://material:material@localhost:5432/material_agents",
)

_BLOG_TABLES = ["blog_media", "blog_slug_redirects", "blog_articles"]


def _asyncpg_url(url: str) -> str:
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+asyncpg://", 1)
    return url


# ── Session-scoped engine — one event loop for all async fixtures ─────────────

@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def pg_engine():
    engine = create_async_engine(_asyncpg_url(_RAW_URL), echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


# ── Truncate all blog tables before each test (autouse) ───────────────────────

@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def clean_tables(pg_engine):
    async with pg_engine.begin() as conn:
        for table in _BLOG_TABLES:
            await conn.execute(text(f"TRUNCATE {table} RESTART IDENTITY CASCADE"))


# ── Per-test session ──────────────────────────────────────────────────────────

@pytest_asyncio.fixture(loop_scope="session")
async def db(pg_engine):
    factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with factory() as session:
        yield session


# ── FastAPI test app wired to the per-test session ────────────────────────────

@pytest_asyncio.fixture(loop_scope="session")
async def client(db: AsyncSession, monkeypatch, tmp_path):
    pw_hash = bcrypt.hashpw(_TEST_PASSWORD.encode(), bcrypt.gensalt()).decode()

    monkeypatch.setenv("AUTH_SECRET", "x" * 32)
    monkeypatch.setenv("ADMIN_USERNAME", "testadmin")
    monkeypatch.setenv("ADMIN_PASSWORD_HASH", pw_hash)
    monkeypatch.setenv("COOKIE_SECURE", "false")
    monkeypatch.setenv("RATELIMIT_LOGIN", "100/minute")
    monkeypatch.setenv("MEDIA_DIR", str(tmp_path / "media"))

    get_storage_backend.cache_clear()
    blog_limiter._enabled = False
    validate_auth_config()

    test_app = FastAPI()
    test_app.include_router(blog_router)
    test_app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

    async def _override_get_db():
        yield db

    test_app.dependency_overrides[get_db] = _override_get_db

    async with AsyncClient(
        transport=ASGITransport(app=test_app), base_url="http://test"
    ) as ac:
        yield ac

    blog_limiter._enabled = True
    get_storage_backend.cache_clear()
