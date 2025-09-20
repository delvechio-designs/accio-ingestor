# accio-ingestor/src/accio_ingestor/schema.py
from __future__ import annotations

from pydantic import BaseModel


class Page(BaseModel):
    page: int
    text: str


class DocumentPayload(BaseModel):
    filename: str
    sha256: str
    pages: list[Page]
