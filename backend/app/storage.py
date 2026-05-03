"""
Central file registry for uploaded tabular data.

Files are written to UPLOAD_DIR on disk and tracked in _registry with
their upload timestamp. Any module can call get_dataframe(file_id) to
load a file without knowing where it lives on disk.

Expiry: files older than TTL_SECONDS are removed by purge_expired(),
which is called by cleanup_loop() every CLEANUP_INTERVAL_SECONDS.
"""

import asyncio
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

UPLOAD_DIR = Path(__file__).parent.parent / "uploads"
TTL_SECONDS = 2 * 60 * 60        # 2 hours
CLEANUP_INTERVAL_SECONDS = 3600  # run every hour

SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".xlsx", ".xls"}

_registry: dict[str, "_FileRecord"] = {}


@dataclass
class _FileRecord:
    file_id: str
    path: Path
    filename: str
    rows: int
    columns: list[str]
    uploaded_at: float  # unix timestamp


def store_file(data: bytes, filename: str) -> _FileRecord:
    """Persist uploaded bytes and register the file. Returns the record."""
    suffix = Path(filename).suffix.lower()
    if suffix not in SUPPORTED_EXTENSIONS:
        raise ValueError(f"Unsupported file type: {suffix!r}")

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    file_id = str(uuid.uuid4())
    dest = UPLOAD_DIR / f"{file_id}{suffix}"
    dest.write_bytes(data)

    df = _load(dest, suffix)
    record = _FileRecord(
        file_id=file_id,
        path=dest,
        filename=filename,
        rows=len(df),
        columns=list(df.columns),
        uploaded_at=time.monotonic(),
    )
    _registry[file_id] = record
    return record


def get_dataframe(file_id: str) -> Optional[pd.DataFrame]:
    """Return the DataFrame for *file_id*, or None if unknown / expired."""
    record = _registry.get(file_id)
    if record is None or not record.path.exists():
        return None
    return _load(record.path, record.path.suffix.lower())


def get_record(file_id: str) -> Optional[_FileRecord]:
    """Return the registry record for *file_id*, or None."""
    return _registry.get(file_id)


def purge_expired() -> int:
    """Delete files older than TTL_SECONDS. Returns the number removed."""
    now = time.monotonic()
    to_delete = [
        fid for fid, rec in _registry.items()
        if now - rec.uploaded_at > TTL_SECONDS
    ]
    for fid in to_delete:
        rec = _registry.pop(fid)
        rec.path.unlink(missing_ok=True)
    return len(to_delete)


async def cleanup_loop() -> None:
    """Background asyncio task: purge expired files on a fixed interval."""
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)
        purge_expired()


# ── internal helpers ────────────────────────────────────────────────────────

def _load(path: Path, suffix: str) -> pd.DataFrame:
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".tsv":
        return pd.read_csv(path, sep="\t")
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError(f"Cannot load {suffix!r}")
