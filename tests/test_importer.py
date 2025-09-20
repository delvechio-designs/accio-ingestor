# accio-ingestor/tests/test_importer.py
from __future__ import annotations

import responses

from accio_ingestor.importer import AccioClient


@responses.activate
def test_importer_post_ok():
    responses.add(responses.POST, "http://localhost:9876/ingest", status=200)
    AccioClient().post_document({"filename": "x", "sha256": "y", "pages": []})
