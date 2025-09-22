# accio-ingestor/src/accio_ingestor/pii.py
from __future__ import annotations

import re


# Conservative masking for common PII-like patterns
_RE_SSN = re.compile(r"\b(\d{3})[- ]?(\d{2})[- ]?(\d{4})\b")
_RE_DOB = re.compile(r"\b(19|20)\d{2}[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12]\d|3[01])\b")
_RE_EMAIL = re.compile(r"\b([A-Za-z0-9._%+-]){1,64}@[A-Za-z0-9.-]{1,255}\.[A-Za-z]{2,}\b")


def _mask_ssn(m: re.Match) -> str:
    return f"***-**-{m.group(3)}"


def _mask_dob(m: re.Match) -> str:
    return "****-**-**"


def _mask_email(_: re.Match) -> str:
    return "<masked-email>"


def redact(text: str) -> str:
    text = _RE_SSN.sub(_mask_ssn, text)
    text = _RE_DOB.sub(_mask_dob, text)
    text = _RE_EMAIL.sub(_mask_email, text)
    return text
