# accio-ingestor/src/accio_ingestor/slack.py
from __future__ import annotations

import json
import queue
import threading
import time
from typing import Dict, Any

import requests

from .config import settings
from .logging_cfg import get_logger
from .pii import redact

log = get_logger(__name__)


class SlackClient:
    _q: "queue.Queue[Dict[str, Any]]" = queue.Queue()
    _started = False
    _lock = threading.Lock()

    def __init__(self) -> None:
        if not settings.SLACK_WEBHOOK_URL:
            # no webhook configured; calls become no-ops
            return
        with SlackClient._lock:
            if not SlackClient._started:
                t = threading.Thread(target=self._run, daemon=True)
                t.start()
                SlackClient._started = True

    def _run(self) -> None:
        while True:
            item = SlackClient._q.get()
            if item is None:  # pragma: no cover
                break
            try:
                requests.post(str(settings.SLACK_WEBHOOK_URL), json=item, timeout=5)
            except Exception as e:
                log.warning(f"slack post failed: {e}")
            time.sleep(0.2)  # rate limit cushion

    def error(self, code: str, ctx: Dict[str, Any]) -> None:
        """Sanitized error notification. No raw text."""
        if not settings.SLACK_WEBHOOK_URL:
            return
        msg = {
            "text": f"Accio Ingestor error: {code}",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Error:* `{code}`\n*File:* `{ctx.get('filename','')}`\n*SHA:* `{ctx.get('sha256','')}`\n*Info:* `{redact(str(ctx.get('message',''))[:300])}`",
                    },
                }
            ],
        }
        SlackClient._q.put(msg)
