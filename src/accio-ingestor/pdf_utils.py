# accio-ingestor/src/accio_ingestor/pdf_utils.py
from __future__ import annotations

from pathlib import Path
from typing import Tuple, List

import fitz  # PyMuPDF
from PIL import Image
from io import BytesIO

from .schema import DocumentPayload, Page
from .ocr import ocr_image_bytes
from .hashing import sha256_file


def extract_pdf_pages_text(path: Path) -> List[Page]:
    pages: List[Page] = []
    with fitz.open(path) as doc:
        for i, page in enumerate(doc):
            text = page.get_text().strip()
            if not text:
                # rasterize at 300 DPI → OCR
                pix = page.get_pixmap(dpi=300, alpha=False)
                img_bytes = pix.tobytes("png")
                text = ocr_image_bytes(img_bytes).strip()
            pages.append(Page(page=i + 1, text=text))
    return pages


def process_file_to_payload(path: Path) -> Tuple[DocumentPayload, bytes, str, str]:
    """
    Returns: (payload, original_bytes, content_type, sha256)
    """
    suffix = path.suffix.lower()
    original_bytes = path.read_bytes()
    sha256 = sha256_file(path)

    if suffix == ".pdf":
        pages = extract_pdf_pages_text(path)
        payload = DocumentPayload(filename=path.name, sha256=sha256, pages=pages)
        return payload, original_bytes, "application/pdf", sha256

    elif suffix in {".jpg", ".jpeg", ".png"}:
        text = ocr_image_bytes(original_bytes)
        pages = [Page(page=1, text=text)]
        payload = DocumentPayload(filename=path.name, sha256=sha256, pages=pages)
        # rudimentary content type
        ct = "image/jpeg" if suffix in {".jpg", ".jpeg"} else "image/png"
        return payload, original_bytes, ct, sha256

    else:
        raise ValueError(f"Unsupported file type: {suffix}")
