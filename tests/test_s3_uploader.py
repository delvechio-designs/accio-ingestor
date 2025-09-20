# accio-ingestor/tests/test_s3_uploader.py
from __future__ import annotations

import os

import boto3
import botocore
import pytest
from moto import mock_aws

from accio_ingestor.s3uploader import S3Uploader
from accio_ingestor.config import settings


@mock_aws
def test_s3_put_with_tags(monkeypatch):
    monkeypatch.setattr(settings, "AWS_REGION", "us-east-1")
    monkeypatch.setattr(settings, "S3_BUCKET", "test-bucket")
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    up = S3Uploader()
    up.put_object_with_retention(
        key="ingests/2025/09/20/original/foo.pdf",
        content=b"abc",
        content_type="application/pdf",
        tags={"retention": "2y"},
    )
    got = s3.get_object(Bucket="test-bucket", Key="ingests/2025/09/20/original/foo.pdf")
    assert got["ResponseMetadata"]["HTTPStatusCode"] == 200
