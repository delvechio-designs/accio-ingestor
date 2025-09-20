# accio-ingestor/src/accio_ingestor/importer.py
from __future__ import annotations

import requests
from typing import Dict, Any

from .config import settings
from .logging_cfg import get_logger

log = get_logger(__name__)


class AccioClient:
    def __init__(self) -> None:
        self.url = str(settings.ACCIO_ENDPOINT)
        self.token = settings.ACCIO_TOKEN
        self.timeout = 15

    def _headers(self) -> Dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h

    def post_document(self, doc: Dict[str, Any]) -> None:
        r = requests.post(self.url, json=doc, headers=self._headers(), timeout=self.timeout)
        if not (200 <= r.status_code < 300):
            raise RuntimeError(f"Accio HTTP {r.status_code}")

    def healthcheck(self) -> bool:
        try:
            r = requests.options(self.url, timeout=5)
            return r.status_code < 500
        except Exception:
            return False
