# accio-ingestor/src/accio_ingestor/logging_cfg.py
from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler
from typing import Any, Dict

from .config import settings
from .pii import redact


class RedactionFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = redact(record.msg)
        return True


def init_logging() -> None:
    logger = logging.getLogger()
    if logger.handlers:
        return
    level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(level)
    file_handler = RotatingFileHandler(settings.LOG_FILE, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(JsonFormatter())
    file_handler.addFilter(RedactionFilter())
    logger.addHandler(file_handler)
    # stderr minimal human readable for debug
    stream = logging.StreamHandler()
    stream.setFormatter(logging.Formatter("[%(levelname)s] %(name)s: %(message)s"))
    stream.addFilter(RedactionFilter())
    logger.addHandler(stream)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "level": record.levelname,
            "name": record.name,
            "msg": record.getMessage(),
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
