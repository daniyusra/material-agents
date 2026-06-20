"""Async SQLAlchemy engine and session factory for the blog.

DATABASE_URL must be a PostgreSQL DSN, e.g.:
    postgresql+asyncpg://user:pass@host/dbname
"""

import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine, AsyncConnection

_engine = None
_session_factory = None


def get_engine():
    global _engine
    if _engine is None:
        url = os.environ["DATABASE_URL"]
        # asyncpg driver requires the +asyncpg scheme
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        elif url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+asyncpg://", 1)
        _engine = create_async_engine(
            url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(get_engine(), expire_on_commit=False)
    return _session_factory


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency — yields a session inside an open transaction."""
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            yield session


@asynccontextmanager
async def open_db() -> AsyncGenerator[AsyncSession, None]:
    """Context manager for use outside FastAPI (e.g. lifespan, scripts)."""
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            yield session


async def close_engine() -> None:
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
