# services/gcs_writer.py
import json
import gzip
from datetime import datetime
from typing import Iterable, Dict, Any, Optional

from google.cloud import storage
from google.oauth2 import service_account


class GCSWriter:
    """
    Streams JSONL data into Google Cloud Storage using constant memory.

    Example:
        gcs = GCSWriter(project_id, cred_path, bucket="my-bucket",
                        prefix="quickbase_exports/", compress=True)
        object_name = gcs.new_object_name()  # e.g., quickbase_exports/qb_records_20250908T120000Z.jsonl.gz
        count = gcs.stream_jsonl(object_name, records_iterable)
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
        """
        Args:
            project_id: GCP project id
            cred_path: path to a service account JSON key
            bucket: GCS bucket name
            prefix: folder/prefix for objects (e.g., "quickbase_exports/")
            compress: if True, writes .jsonl.gz with Content-Encoding=gzip
            chunk_size_mb: resumable upload chunk size (MB)
        """
        creds = service_account.Credentials.from_service_account_file(cred_path)
        self.client = storage.Client(credentials=creds, project=project_id)
        self.bucket_name = bucket
        self.bucket = self.client.bucket(bucket)
        self.prefix = prefix.rstrip("/")
        self.compress = bool(compress)
        self.chunk_size = max(1, int(chunk_size_mb)) * 1024 * 1024  # bytes

    # ------------ Public API ------------

    def new_object_name(self, basename: str = "qb_records", ts: Optional[datetime] = None) -> str:
        """
        Create a timestamped object name under the configured prefix.

        Returns:
            e.g., "quickbase_exports/qb_records_20250908T120000Z.jsonl.gz"
        """
        ts = ts or datetime.utcnow()
        stamp = ts.strftime("%Y%m%dT%H%M%SZ")
        ext = "jsonl.gz" if self.compress else "jsonl"
        return f"{self.prefix}/{basename}_{stamp}.{ext}"

    def stream_jsonl(self, object_name: str, records: Iterable[Dict[str, Any]]) -> int:
        """
        Stream an iterable of dicts to a JSONL (optionally gzipped) object in GCS.

        Args:
            object_name: path within the bucket (no gs://)
            records: iterable of Python dicts; each becomes one JSON line

        Returns:
            Number of records written.
        """
        blob = self.bucket.blob(object_name)
        blob.chunk_size = self.chunk_size
        # NDJSON mime; BigQuery accepts this for newline-delimited JSON
        blob.content_type = "application/x-ndjson"
        if self.compress:
            blob.content_encoding = "gzip"

        count = 0
        with blob.open("wb") as raw:
            writer = gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=6, mtime=0) if self.compress else raw
            try:
                for rec in records:
                    # compact separators; ensure_ascii False preserves UTF-8
                    line = (json.dumps(rec, ensure_ascii=False, default=str) + "\n").encode("utf-8")
                    writer.write(line)
                    count += 1
            finally:
                # If gzip, make sure footer is flushed
                if writer is not raw:
                    writer.close()
        return count

    def exists(self, object_name: str) -> bool:
        """Return True if the object exists in the bucket."""
        return self.bucket.blob(object_name).exists()
