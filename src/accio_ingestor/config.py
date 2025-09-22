from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import AnyUrl, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # App folders
    WATCH_DIR: str = "./incoming"
    PROCESSED_DIR: str = "./processed"
    FAILED_DIR: str = "./failed"

    # Slack
    SLACK_WEBHOOK_URL: Optional[AnyUrl] = None

    # Accio
    ACCIO_ENDPOINT: AnyUrl = Field(default="http://localhost:9876/ingest")
    ACCIO_TOKEN: Optional[str] = None

    # AWS/S3
    AWS_REGION: str = "us-east-1"
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    AWS_SESSION_TOKEN: Optional[str] = None
    S3_BUCKET: Optional[str] = None
    S3_PREFIX: str = "ingests/"
    S3_SSE_KMS_KEY_ID: Optional[str] = None
    S3_OBJECT_LOCK_MODE: Optional[str] = None
    S3_OBJECT_LOCK_DAYS: int = 730

    # OCR
    TESSERACT_CMD: Optional[str] = None

    # Logging
    LOG_FILE: str = "app.log"
    LOG_JSONL: str = "audit.jsonl"
    LOG_LEVEL: str = "INFO"

    # Queue encryption (optional)
    QUEUE_ENCRYPTION_KEY: Optional[str] = None

    # Licensing
    LICENSE_KEY: Optional[str] = None

    # Generic, user-configurable hooks/keys (for sellable SKU)
    CUSTOM_WEBHOOK_1: Optional[AnyUrl] = None
    CUSTOM_WEBHOOK_2: Optional[AnyUrl] = None
    CUSTOM_WEBHOOK_3: Optional[AnyUrl] = None
    API_KEY_1: Optional[str] = None
    API_KEY_2: Optional[str] = None
    API_KEY_3: Optional[str] = None

    @field_validator("S3_OBJECT_LOCK_MODE")
    @classmethod
    def validate_lock_mode(cls, v: Optional[str]) -> Optional[str]:
        if not v:
            return None
        v = v.upper()
        if v not in {"GOVERNANCE", "COMPLIANCE"}:
            raise ValueError("S3_OBJECT_LOCK_MODE must be GOVERNANCE, COMPLIANCE or empty")
        return v


settings = Settings()


def ensure_dirs() -> None:
    Path(settings.WATCH_DIR).mkdir(parents=True, exist_ok=True)
    Path(settings.PROCESSED_DIR).mkdir(parents=True, exist_ok=True)
    Path(settings.FAILED_DIR).mkdir(parents=True, exist_ok=True)
    Path(settings.LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
    Path(settings.LOG_JSONL).parent.mkdir(parents=True, exist_ok=True)


def reload_from_env(env_path: str | None = None) -> None:
    """Reload settings from .env so GUI changes take effect without restart."""
    global settings
    settings = Settings(_env_file=env_path) if env_path else Settings()  # type: ignore
    ensure_dirs()
