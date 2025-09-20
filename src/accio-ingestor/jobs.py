# accio-ingestor/src/accio_ingestor/jobs.py
from __future__ import annotations

import base64
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

from .logging_cfg import get_logger
from .slack import SlackClient
from .s3uploader import S3Uploader
from .importer import AccioClient
from .audit import audit
from .pii import redact
from .config import settings

log = get_logger(__name__)


def enqueue_ingest_jobs(queue, payload_model, sha256: str, original_bytes: bytes, content_type: str, filename: str) -> None:
    json_bytes = json.dumps(payload_model.model_dump()).encode("utf-8")

    # S3 original
    queue.enqueue(
        "S3_PUT",
        {
            "kind": "original",
            "filename": filename,
            "content_type": content_type,
            "bytes_b64": base64.b64encode(original_bytes).decode("ascii"),
            "tags": {"retention": "2y", "app": "accio-ingestor"},
        },
        sha256=sha256,
    )
    # S3 extracted JSON
    queue.enqueue(
        "S3_PUT",
        {
            "kind": "extracted",
            "filename": f"{sha256}.json",
            "content_type": "application/json",
            "bytes_b64": base64.b64encode(json_bytes).decode("ascii"),
            "tags": {"retention": "2y", "app": "accio-ingestor"},
        },
        sha256=sha256,
    )
    # Accio post
    queue.enqueue(
        "ACCIO",
        {"json": payload_model.model_dump()},
        sha256=sha256,
    )
    audit.append("enqueued", filename=filename, sha256=sha256)


def handle_job(type_: str, payload: Dict[str, Any]) -> None:
    if type_ == "S3_PUT":
        _job_s3_put(payload)
    elif type_ == "ACCIO":
        _job_accio(payload)
    else:
        raise ValueError(f"Unknown job type {type_}")


def _job_s3_put(payload: Dict[str, Any]) -> None:
    kind = payload["kind"]
    filename = payload["filename"]
    content = base64.b64decode(payload["bytes_b64"])
    content_type = payload["content_type"]
    tags = payload.get("tags", {})
    uploader = S3Uploader()
    today = datetime.now(timezone.utc)
    key = f"{settings.S3_PREFIX}{today.year:04d}/{today.month:02d}/{today.day:02d}/{kind}/{filename}"
    try:
        uploader.put_object_with_retention(key, content, content_type, tags)
        audit.append("s3_put", filename=filename, sha256=tags.get("sha256", ""), job_id=None, status="ok", key=key)
    except Exception as e:
        # On final failure, Slack will be handled by queue via attempts policy; raise to trigger retry
        raise


def _job_accio(payload: Dict[str, Any]) -> None:
    client = AccioClient()
    doc = payload["json"]
    try:
        client.post_document(doc)
        audit.append("accio_post", filename=doc.get("filename"), sha256=doc.get("sha256"), status="ok")
    except Exception as e:
        # only sanitized Slack upon final failure is orchestrated by the worker attempts; re-raise
        raise
