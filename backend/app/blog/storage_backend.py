"""
Storage backend abstraction for blog media.

Interface: LocalStorageBackend (default) stores files on the persistent Fly.io
volume at MEDIA_DIR. An S3StorageBackend stub is included for future swap.
Gate via MEDIA_BACKEND=local|s3.
"""

import os
from abc import ABC, abstractmethod
from functools import lru_cache
from pathlib import Path


class StorageBackend(ABC):
    @abstractmethod
    def save(self, content: bytes, filename: str, content_type: str) -> str:
        """Persist content and return a public URL path (e.g. '/media/abc.webp')."""

    @abstractmethod
    def delete(self, url: str) -> None:
        """Delete the file identified by its public URL path."""

    @abstractmethod
    def url_to_path(self, url: str) -> str:
        """Return a filesystem path (for serving via StaticFiles) given a URL path."""


class LocalStorageBackend(StorageBackend):
    def __init__(self, media_dir: Path, url_prefix: str = "/media") -> None:
        self._dir = media_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._prefix = url_prefix.rstrip("/")

    def save(self, content: bytes, filename: str, content_type: str) -> str:
        (self._dir / filename).write_bytes(content)
        return f"{self._prefix}/{filename}"

    def delete(self, url: str) -> None:
        filename = url.split("/")[-1]
        path = self._dir / filename
        if path.exists():
            path.unlink()

    def url_to_path(self, url: str) -> str:
        filename = url.split("/")[-1]
        return str(self._dir / filename)


class S3StorageBackend(StorageBackend):
    """Stub — swap in when MEDIA_BACKEND=s3. Fill in with boto3/aiobotocore."""

    def save(self, content: bytes, filename: str, content_type: str) -> str:  # pragma: no cover
        raise NotImplementedError("S3StorageBackend is not yet implemented")

    def delete(self, url: str) -> None:  # pragma: no cover
        raise NotImplementedError("S3StorageBackend is not yet implemented")

    def url_to_path(self, url: str) -> str:  # pragma: no cover
        raise NotImplementedError("S3 files are not served via filesystem path")


@lru_cache(maxsize=1)
def get_storage_backend() -> StorageBackend:
    backend = os.getenv("MEDIA_BACKEND", "local").lower()
    if backend == "s3":
        return S3StorageBackend()
    default_dir = str(Path(__file__).parent.parent.parent / "media")
    media_dir = Path(os.getenv("MEDIA_DIR", default_dir))
    return LocalStorageBackend(media_dir)


def get_media_dir() -> Path:
    backend = get_storage_backend()
    if isinstance(backend, LocalStorageBackend):
        return backend._dir
    return Path(os.getenv("MEDIA_DIR", str(Path(__file__).parent.parent.parent / "media")))
