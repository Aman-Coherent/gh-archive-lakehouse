"""Filesystem abstraction over local disk and S3-compatible object stores.

WHY fsspec: STORAGE_BACKEND=local (dev) and STORAGE_BACKEND=r2 (CI/prod) must
run through exactly the same ingest/retention code, never branching on which
backend is active. fsspec gives both a single file-like interface, so that
branch only has to happen once, here, at construction time.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.config import Settings


class Storage:
    """Reads/writes Parquet partitions against the configured backend (local or R2)."""

    def __init__(self, settings: Settings) -> None:
        self._backend = settings.storage_backend
        if self._backend == "local":
            self._fs = fsspec.filesystem("file")
            # WHY resolve to an absolute path immediately: fsspec's
            # LocalFileSystem.find() always returns absolute, OS-normalized
            # paths regardless of what root you pass it. LOCAL_DATA_ROOT
            # defaults to the relative "./data" — stripping that literal
            # relative string's length off an absolute result silently cuts
            # the wrong characters (confirmed directly: it produced a
            # mangled path like ".../data/s/aman/..." instead of
            # ".../data/bronze/..."). Resolving once here keeps self._root
            # consistent with what find() actually returns everywhere else
            # in this class.
            self._root = Path(settings.local_data_root).resolve().as_posix()
        else:
            # WHY: Cloudflare R2 speaks the S3 API, so s3fs works against it
            # unmodified — only the endpoint URL differs from real AWS S3.
            self._fs = fsspec.filesystem(
                "s3",
                key=settings.r2_access_key_id,
                secret=settings.r2_secret_access_key,
                client_kwargs={
                    "endpoint_url": f"https://{settings.r2_account_id}.r2.cloudflarestorage.com",
                },
            )
            self._root = settings.r2_bucket

    def _full_path(self, path: str) -> str:
        return f"{self._root.rstrip('/')}/{path.lstrip('/')}"

    def write_parquet_atomic(self, table: pa.Table, path: str) -> int:
        """Write a Parquet file so readers never observe a partially-written partition.

        WHY temp-then-move: writing straight to the final path leaves a window
        where a crash mid-write leaves a truncated file that downstream readers
        (DuckDB glob reads, dbt) would treat as a real, corrupt partition. Writing
        to a hidden temp path first and moving it into place means the final path
        only ever comes into existence once the file is fully valid — a move is
        effectively instantaneous, so there's no "half-renamed" state to observe.

        Returns the actual compressed size in bytes, as written to storage.
        """
        full_path = self._full_path(path)
        tmp_path = f"{full_path}.tmp-{uuid.uuid4().hex}"

        if self._backend == "local":
            # WHY: object stores have no real directories: a key like
            # "a/b/c.parquet" can be written with no "a/b/" ever created.
            # Local disk does need the parent directory to exist first.
            self._fs.makedirs(full_path.rsplit("/", 1)[0], exist_ok=True)

        with self._fs.open(tmp_path, "wb") as f:
            pq.write_table(table, f, compression="zstd")
        bytes_written = self._fs.size(tmp_path)
        self._fs.mv(tmp_path, full_path)
        return bytes_written

    def exists(self, path: str) -> bool:
        return bool(self._fs.exists(self._full_path(path)))

    def size(self, path: str) -> int:
        return int(self._fs.size(self._full_path(path)))

    def list_partitions(self, prefix: str) -> list[str]:
        """List partition files under a prefix, as backend-relative paths."""
        full_prefix = self._full_path(prefix)
        if not self._fs.exists(full_prefix):
            return []
        root_len = len(self._root.rstrip("/")) + 1
        return sorted(p[root_len:] for p in self._fs.find(full_prefix) if p.endswith(".parquet"))

    def delete_prefix(self, prefix: str) -> int:
        """Delete every file under a prefix; returns the count of files removed."""
        full_prefix = self._full_path(prefix)
        if not self._fs.exists(full_prefix):
            return 0
        paths = self._fs.find(full_prefix)
        for p in paths:
            self._fs.rm(p)
        return len(paths)
