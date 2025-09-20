# accio-ingestor/src/accio_ingestor/audit.py
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict

from .config import settings


class AuditLog:
    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path or settings.LOG_JSONL)
        self.max_bytes = 10_000_000

    def _rotate_if_needed(self) -> None:
        try:
            if self.path.exists() and self.path.stat().st_size > self.max_bytes:
                ts = time.strftime("%Y%m%d-%H%M%S")
                self.path.rename(self.path.with_name(f"{self.path.stem}-{ts}{self.path.suffix}"))
        except Exception:
            # best effort; never raise
            pass

    def append(self, event: str, **fields: Any) -> None:
        self._rotate_if_needed()
        rec: Dict[str, Any] = {"ts": time.time(), "event": event}
        rec.update(fields)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


audit = AuditLog()
