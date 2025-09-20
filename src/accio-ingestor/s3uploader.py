# accio-ingestor/src/accio_ingestor/s3uploader.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
import urllib.parse as _url

import boto3

from .config import settings
from .logging_cfg import get_logger

log = get_logger(__name__)


class S3Uploader:
    def __init__(self):
        self.client = boto3.client(
            "s3",
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            aws_session_token=settings.AWS_SESSION_TOKEN,
        )
        self.region = settings.AWS_REGION
        self.bucket = settings.S3_BUCKET

        if not self.bucket:
            raise ValueError("S3_BUCKET must be set to enable S3 uploads")

    def put_object_with_retention(
        self,
        key: str,
        content: bytes,
        content_type: str,
        tags: Dict[str, str],
    ) -> None:
        kwargs = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": content,
            "ContentType": content_type,
            "ACL": "bucket-owner-full-control",
        }

        if settings.S3_SSE_KMS_KEY_ID:
            kwargs["ServerSideEncryption"] = "aws:kms"
            kwargs["SSEKMSKeyId"] = settings.S3_SSE_KMS_KEY_ID
        else:
            kwargs["ServerSideEncryption"] = "AES256"

        if tags:
            kwargs["Tagging"] = _url.urlencode(tags)

        if settings.S3_OBJECT_LOCK_MODE:
            # Only valid if bucket has object lock; guard with try/except
            try:
                retain_until = datetime.now(timezone.utc) + timedelta(days=settings.S3_OBJECT_LOCK_DAYS)
                kwargs["ObjectLockMode"] = settings.S3_OBJECT_LOCK_MODE
                kwargs["ObjectLockRetainUntilDate"] = retain_until
            except Exception as e:
                log.warning(f"Object Lock not applied: {e}")

        self.client.put_object(**kwargs)
