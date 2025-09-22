from __future__ import annotations

import os
from pathlib import Path
from typing import Dict

ENV_KEYS_ORDER: list[str] = [
    "WATCH_DIR",
    "PROCESSED_DIR",
    "FAILED_DIR",
    "SLACK_WEBHOOK_URL",
    "ACCIO_ENDPOINT",
    "ACCIO_TOKEN",
    "AWS_REGION",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "S3_BUCKET",
    "S3_PREFIX",
    "S3_SSE_KMS_KEY_ID",
    "S3_OBJECT_LOCK_MODE",
    "S3_OBJECT_LOCK_DAYS",
    "TESSERACT_CMD",
    "LOG_FILE",
    "LOG_JSONL",
    "LOG_LEVEL",
    "QUEUE_ENCRYPTION_KEY",
    "LICENSE_KEY",
    # New: generic hooks/keys for customer integrations
    "CUSTOM_WEBHOOK_1",
    "CUSTOM_WEBHOOK_2",
    "CUSTOM_WEBHOOK_3",
    "API_KEY_1",
    "API_KEY_2",
    "API_KEY_3",
]

SENSITIVE_KEYS = {
    "SLACK_WEBHOOK_URL",
    "ACCIO_TOKEN",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "S3_SSE_KMS_KEY_ID",
    "QUEUE_ENCRYPTION_KEY",
    "LICENSE_KEY",
    "CUSTOM_WEBHOOK_1",
    "CUSTOM_WEBHOOK_2",
    "CUSTOM_WEBHOOK_3",
    "API_KEY_1",
    "API_KEY_2",
    "API_KEY_3",
}


def load_env_file(path: Path) -> Dict[str, str]:
    """Parse a simple .env (KEY=VALUE). Ignores comments/blanks."""
    data: Dict[str, str] = {}
    if not path.exists():
        return data
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        data[k.strip()] = v.strip()
    return data


def save_env_file(path: Path, kv: Dict[str, str]) -> None:
    """Write .env atomically; 0600 where supported."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    seen = set()
    for k in ENV_KEYS_ORDER:
        if k in kv:
            lines.append(f"{k}={kv[k]}")
            seen.add(k)
    for k, v in kv.items():
        if k not in seen:
            lines.append(f"{k}={v}")

    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    try:
        if os.name != "nt":
            os.chmod(tmp, 0o600)
        tmp.replace(path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass


def masked_preview(kv: Dict[str, str]) -> Dict[str, str]:
    """Return a masked copy for diagnostics UI."""
    out: Dict[str, str] = {}
    for k, v in kv.items():
        if k in SENSITIVE_KEYS and v:
            out[k] = "****" if len(v) <= 8 else f"{v[:4]}****{v[-4:]}"
        else:
            out[k] = v
    return out
