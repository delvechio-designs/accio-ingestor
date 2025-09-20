# accio-ingestor/src/accio_ingestor/config.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, AnyUrl, ValidationError, field_validator, BaseSettings


class Settings(BaseSettings):
    # App folders
    WATCH_DIR: str = "./incoming"
    PROCESSED_DIR: str = "./processed"
    FAILED_DIR: str = "./failed"

    # Slack
    SLACK_WEBHOOK_URL: Optional[AnyUrl] = None

    # Accio
    ACCIO_ENDPOINT: AnyUrl = Field(default="http://localhost:9876/ingest")
    ACCIO_TOKEN: str | None = None

    # AWS/S3
    AWS_REGION: str = "us-east-1"
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    AWS_SESSION_TOKEN: Optional[str] = None
    S3_BUCKET: Optional[str] = None
    S3_PREFIX: str = "ingests/"
    S3_SSE_KMS_KEY_ID: Optional[str] = None
    S3_OBJECT_LOCK_MODE: Optional[str] = None  # "GOVERNANCE", "COMPLIANCE", or None
    S3_OBJECT_LOCK_DAYS: int = 730

    # OCR
    TESSERACT_CMD: Optional[str] = None

    # Logging
    LOG_FILE: str = "app.log"
    LOG_JSONL: str = "audit.jsonl"
    LOG_LEVEL: str = "INFO"

    # Queue encryption (optional)
    QUEUE_ENCRYPTION_KEY: Optional[str] = None  # 32-byte base64 urlsafe recommended

    @field_validator("S3_OBJECT_LOCK_MODE")
    @classmethod
    def validate_lock_mode(cls, v: Optional[str]) -> Optional[str]:
        if not v:
            return None
        v = v.upper()
        if v not in {"GOVERNANCE", "COMPLIANCE"}:
            raise ValueError("S3_OBJECT_LOCK_MODE must be GOVERNANCE, COMPLIANCE or empty")
        return v

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()


def ensure_dirs() -> None:
    Path(settings.WATCH_DIR).mkdir(parents=True, exist_ok=True)
    Path(settings.PROCESSED_DIR).mkdir(parents=True, exist_ok=True)
    Path(settings.FAILED_DIR).mkdir(parents=True, exist_ok=True)
    Path(settings.LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
    Path(settings.LOG_JSONL).parent.mkdir(parents=True, exist_ok=True)
