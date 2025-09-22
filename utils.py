import re
from datetime import datetime, timezone

# Matches qb_records_20250920T145059Z.jsonl(.gz)
_STAMP_RX = re.compile(r"_(\d{8}T\d{6}Z)")

def gcs_object_epoch_ms(object_name: str) -> int:
    """Extract YYYYMMDDTHHMMSSZ from the object name and return epoch ms (UTC)."""
    name = object_name.rsplit("/", 1)[-1]             # filename only
    m = _STAMP_RX.search(name)
    if not m:
        raise ValueError(f"No timestamp like YYYYMMDDTHHMMSSZ in: {object_name}")
    stamp = m.group(1)                                # e.g. 20250920T145059Z
    dt = datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)
