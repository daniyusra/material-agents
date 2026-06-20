"""
Seed the blog database with sample articles for local development.

Usage:
    DATABASE_URL=postgresql+asyncpg://... uv run python scripts/seed_blog.py
"""

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.blog.database import get_db


ARTICLES = [
    {
        "title": "Getting Started with FastAPI",
        "excerpt": "A quick tour of FastAPI's key features and why it's a great choice for Python APIs.",
        "body": (
            "# Getting Started with FastAPI\n\n"
            "FastAPI is a modern, fast web framework for building APIs with Python.\n\n"
            "## Key Features\n\n"
            "- Automatic OpenAPI docs\n"
            "- Native async support\n"
            "- Pydantic validation\n\n"
            "Getting started is as simple as `pip install fastapi`.\n"
        ),
        "tags": ["python", "fastapi", "api"],
        "status": "published",
    },
    {
        "title": "Why PostgreSQL Over SQLite in Production",
        "excerpt": "SQLite is great for development, but PostgreSQL is the right choice when you go to production.",
        "body": (
            "# Why PostgreSQL Over SQLite in Production\n\n"
            "SQLite stores everything in a single file and is perfect for local dev.\n"
            "But in production you need concurrent writes, proper connection pooling,\n"
            "and a type system that scales.\n\n"
            "PostgreSQL gives you all of that and more.\n"
        ),
        "tags": ["postgresql", "database", "production"],
        "status": "published",
    },
    {
        "title": "Draft: Upcoming Feature Deep-Dive",
        "excerpt": "Work in progress — not published yet.",
        "body": "# Coming Soon\n\nThis article is still being written.\n",
        "tags": ["draft"],
        "status": "draft",
    },
]


async def main() -> None:
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    from app.blog.models import Base
    from app.blog import crud

    url = os.environ.get("DATABASE_URL", "")
    if not url:
        print("ERROR: DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)

    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)

    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(engine, expire_on_commit=False)
    now = int(time.time())

    async with factory() as session:
        async with session.begin():
            for data in ARTICLES:
                article = await crud.create_article(session, data["title"])
                update_fields = {
                    "excerpt": data["excerpt"],
                    "body": data["body"],
                    "tags": data["tags"],
                    "status": data["status"],
                }
                if data["status"] == "published":
                    update_fields["published_at"] = now
                await crud.update_article(session, article["id"], update_fields)
                print(f"  seeded: {data['title']!r} [{data['status']}]")

    await engine.dispose()
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
