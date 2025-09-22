from __future__ import annotations

import json
from typing import Any, Dict, Optional

import requests

from .config import settings


class AccioClient:
    """
    Simple HTTP client for posting extracted JSON to the Accio endpoint.
    Defaults to values from settings but allows per-call overrides via __init__.
    """

    def __init__(self, endpoint: Optional[str] = None, token: Optional[str] = None, timeout_s: int = 15) -> None:
        self.endpoint = endpoint or str(settings.ACCIO_ENDPOINT)
        self.token = token or settings.ACCIO_TOKEN
        self.timeout_s = timeout_s

    def post_document(self, payload: Dict[str, Any]) -> None:
        """
        POST JSON payload to Accio.
        - Auth: Bearer <token> if provided
        - Treat any 2xx as success, otherwise raise for status
        """
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        # Ensure we’re posting to /ingest (handle if user pastes base URL)
        url = self.endpoint
        if url.endswith("/"):
            url = url[:-1]
        if not url.endswith("/ingest"):
            url = f"{url}/ingest"

        resp = requests.post(url, data=json.dumps(payload), headers=headers, timeout=self.timeout_s)
        if not (200 <= resp.status_code < 300):
            # Raise an informative error for GUI
            raise RuntimeError(f"Accio HTTP {resp.status_code}: {resp.text[:200]}")
