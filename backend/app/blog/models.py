"""SQLAlchemy 2.x declarative models for the blog."""

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    ForeignKey,
    Index,
    Text,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Article(Base):
    __tablename__ = "blog_articles"
    __table_args__ = (
        CheckConstraint("status IN ('draft', 'published')", name="ck_blog_articles_status"),
        Index("idx_blog_articles_slug", "slug"),
        Index("idx_blog_articles_status_pub", "status", "published_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    excerpt: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    meta_description: Mapped[str | None] = mapped_column(Text)
    cover_image_url: Mapped[str | None] = mapped_column(Text)
    og_image_url: Mapped[str | None] = mapped_column(Text)
    tags: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False, server_default="{}")
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default="draft")
    published_at: Mapped[int | None] = mapped_column(BigInteger)
    created_at: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at: Mapped[int] = mapped_column(BigInteger, nullable=False)
    author: Mapped[str] = mapped_column(Text, nullable=False, server_default="Admin")
    slug_frozen: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")

    slug_redirects: Mapped[list["SlugRedirect"]] = relationship(
        back_populates="article", cascade="all, delete-orphan"
    )
    media: Mapped[list["Media"]] = relationship(back_populates="article")


class SlugRedirect(Base):
    __tablename__ = "blog_slug_redirects"

    old_slug: Mapped[str] = mapped_column(Text, primary_key=True)
    article_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("blog_articles.id", ondelete="CASCADE"), nullable=False
    )
    created_at: Mapped[int] = mapped_column(BigInteger, nullable=False)

    article: Mapped["Article"] = relationship(back_populates="slug_redirects")


class Media(Base):
    __tablename__ = "blog_media"
    __table_args__ = (Index("idx_blog_media_article", "article_id"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    article_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("blog_articles.id", ondelete="SET NULL")
    )
    filename: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    alt_text: Mapped[str | None] = mapped_column(Text)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[int] = mapped_column(BigInteger, nullable=False)

    article: Mapped["Article | None"] = relationship(back_populates="media")
