# accio-ingestor/tests/test_pdf_ocr.py
from __future__ import annotations

from pathlib import Path

import pytest

from accio_ingestor.pdf_utils import process_file_to_payload


def test_process_pdf_unsupported(tmp_path: Path):
    p = tmp_path / "a.txt"
    p.write_text("x")
    with pytest.raises(ValueError):
        process_file_to_payload(p)
