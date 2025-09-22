# accio-ingestor/src/accio_ingestor/ocr.py
from __future__ import annotations

import os
from typing import Optional

import pytesseract
from PIL import Image
from io import BytesIO

from .config import settings


def _ensure_tesseract_configured() -> None:
    if settings.TESSERACT_CMD:
        pytesseract.pytesseract.tesseract_cmd = settings.TESSERACT_CMD


def ocr_image_bytes(img_bytes: bytes, lang: Optional[str] = "eng") -> str:
    _ensure_tesseract_configured()
    with BytesIO(img_bytes) as bio:
        img = Image.open(bio)
        text = pytesseract.image_to_string(img, lang=lang or "eng")
        return text or ""
