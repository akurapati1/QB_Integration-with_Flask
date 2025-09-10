# services/gcs_writer.py
import json
import gzip
from contextlib import contextmanager
from datetime import datetime
from typing import Iterable, Dict, Any, Optional

from google.cloud import storage
from google.oauth2 import service_account

class GCSWriter:
    """
    JSONL writer to Google Cloud Storage.

    - open_jsonl(object_name): open ONE object once (with ignore_flush=True).
    - write_batch(writer, records): write a page of dicts to that object.
    """

    def __init__(
        self,
        project_id: str,
        cred_path: str,
        bucket: str,
        prefix: str,
        compress: bool = True,
        chunk_size_mb: int = 5,
    ) -> None:
        creds = service_account.Credentials.from_service_account_file(cred_path)
        self.client = storage.Client(credentials=creds, project=project_id)
        self.bucket_name = bucket
        self.bucket = self.client.bucket(bucket)
        self.prefix = prefix.rstrip("/")
        self.compress = bool(compress)
        self.chunk_size = max(1, int(chunk_size_mb)) * 1024 * 1024  # bytes

    def new_object_name(self, basename: str = "qb_records", ts: Optional[datetime] = None) -> str:
        ts = ts or datetime.utcnow()
        stamp = ts.strftime("%Y%m%dT%H%M%SZ")
        ext = "jsonl.gz" if self.compress else "jsonl"
        return f"{self.prefix}/{basename}_{stamp}.{ext}"

    @contextmanager
    def open_jsonl(self, object_name: str):
        """
        Open a JSONL (optionally gzipped) object for writing once.
        Uses ignore_flush=True to avoid flush errors during resumable upload.
        """
        blob = self.bucket.blob(object_name)
        blob.chunk_size = self.chunk_size
        blob.content_type = "application/x-ndjson"
        if self.compress:
            blob.content_encoding = "gzip"

        # IMPORTANT: ignore_flush=True prevents the “Cannot flush...” error.
        raw = blob.open("wb", ignore_flush=True)
        writer = gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=6, mtime=0) if self.compress else raw

        try:
            yield writer
        finally:
            # Close cleanly; ensure gzip footer and upload finalization.
            try:
                writer.close()
            except Exception:
                pass
            try:
                if raw is not writer:  # when gzip wrapped
                    raw.close()
            except Exception:
                pass

    def write_batch(self, writer, records: Iterable[Dict[str, Any]]) -> int:
        """
        Write a batch of dicts as JSONL lines. No per-batch flush (not supported).
        """
        count = 0
        for rec in records:
            line = (json.dumps(rec, ensure_ascii=False, default=str) + "\n").encode("utf-8")
            writer.write(line)
            count += 1
        # Do NOT writer.flush(): BlobWriter does not support flush during resumable uploads.
        return count

    # Optional compatibility helper
    def stream_jsonl(self, object_name: str, records: Iterable[Dict[str, Any]]) -> int:
        with self.open_jsonl(object_name) as w:
            return self.write_batch(w, records)

    def exists(self, object_name: str) -> bool:
        return self.bucket.blob(object_name).exists()
