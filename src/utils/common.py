import re
import unicodedata
from datetime import datetime, timezone
from typing import Optional


def slugify_query(query: str, max_length: int = 40) -> str:
    normalized = unicodedata.normalize("NFKD", query)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "_", ascii_text.strip().lower()).strip("_")
    return slug[:max_length].rstrip("_") or "query"


def get_batch_id(batch_id: Optional[str] = None) -> str:
    return batch_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
