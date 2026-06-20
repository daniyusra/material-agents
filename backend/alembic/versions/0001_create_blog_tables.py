"""create blog tables

Revision ID: 0001
Revises:
Create Date: 2026-06-17
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY

revision: str = "0001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "blog_articles",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("body", sa.Text(), nullable=False, server_default=""),
        sa.Column("excerpt", sa.Text(), nullable=False, server_default=""),
        sa.Column("meta_description", sa.Text(), nullable=True),
        sa.Column("cover_image_url", sa.Text(), nullable=True),
        sa.Column("og_image_url", sa.Text(), nullable=True),
        sa.Column("tags", ARRAY(sa.Text()), nullable=False, server_default="{}"),
        sa.Column("status", sa.Text(), nullable=False, server_default="draft"),
        sa.Column("published_at", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.BigInteger(), nullable=False),
        sa.Column("updated_at", sa.BigInteger(), nullable=False),
        sa.Column("author", sa.Text(), nullable=False, server_default="Admin"),
        sa.Column("slug_frozen", sa.Boolean(), nullable=False, server_default="false"),
        sa.CheckConstraint("status IN ('draft', 'published')", name="ck_blog_articles_status"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),
    )
    op.create_index("idx_blog_articles_slug", "blog_articles", ["slug"])
    op.create_index("idx_blog_articles_status_pub", "blog_articles", ["status", "published_at"])

    op.create_table(
        "blog_slug_redirects",
        sa.Column("old_slug", sa.Text(), nullable=False),
        sa.Column("article_id", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(["article_id"], ["blog_articles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("old_slug"),
    )

    op.create_table(
        "blog_media",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("article_id", sa.BigInteger(), nullable=True),
        sa.Column("filename", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("alt_text", sa.Text(), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(["article_id"], ["blog_articles.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_blog_media_article", "blog_media", ["article_id"])


def downgrade() -> None:
    op.drop_index("idx_blog_media_article", table_name="blog_media")
    op.drop_table("blog_media")
    op.drop_table("blog_slug_redirects")
    op.drop_index("idx_blog_articles_status_pub", table_name="blog_articles")
    op.drop_index("idx_blog_articles_slug", table_name="blog_articles")
    op.drop_table("blog_articles")
