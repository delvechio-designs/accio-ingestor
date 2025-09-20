# accio-ingestor/tests/test_queue_retry.py
from __future__ import annotations

import json
from pathlib import Path

import responses

from accio_ingestor.queue import JobQueue
from accio_ingestor.jobs import handle_job


@responses.activate
def test_accio_retry_then_success(monkeypatch):
    # First call 500, second 200
    responses.add(responses.POST, "http://localhost:9876/ingest", status=500)
    responses.add(responses.POST, "http://localhost:9876/ingest", status=200)

    payload = {"json": {"filename": "x", "sha256": "s", "pages": []}}
    # We simulate a first failure by calling handle_job under failing mock…
    try:
        handle_job("ACCIO", payload)
        # If it didn't raise, force a failure to ensure the second branch is exercised
        raise AssertionError("Expected RuntimeError on first mocked failure")
    except RuntimeError:
        pass

    # …then reset mocks and succeed
    responses.reset()
    responses.add(responses.POST, "http://localhost:9876/ingest", status=200)
    handle_job("ACCIO", payload)
