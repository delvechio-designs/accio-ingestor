# accio-ingestor/tests/test_end_to_end.py
from __future__ import annotations

import time
from pathlib import Path

import pytest

from accio_ingestor.queue import JobQueue
from accio_ingestor.watcher import WatchRunner
from accio_ingestor.config import settings


def test_e2e_starts_and_moves(tmp_path: Path, monkeypatch):
    watch = tmp_path / "incoming"
    proc = tmp_path / "processed"
    fail = tmp_path / "failed"
    watch.mkdir()
    proc.mkdir()
    fail.mkdir()

    monkeypatch.setattr(settings, "WATCH_DIR", str(watch))
    monkeypatch.setattr(settings, "PROCESSED_DIR", str(proc))
    monkeypatch.setattr(settings, "FAILED_DIR", str(fail))
    # Disable S3 and Accio by not enqueuing real jobs; we'll drop a .png which OCR reads (no networks here)
    # Create a dummy png (empty content may error; so create small valid PNG)
    png = watch / "img.png"
    # Minimal 1x1 PNG
    png.write_bytes(
        bytes.fromhex(
            "89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de0000000a49444154789c63600000020001734a0d0b0000000049454e44ae426082"
        )
    )

    queue = JobQueue()
    queue.start_worker()
    runner = WatchRunner(queue)
    try:
        import threading

        t = threading.Thread(target=runner.start_blocking, daemon=True)
        t.start()
        # let watcher pick it up and move to processed (even if jobs fail, movement should occur)
        for _ in range(30):
            if (proc / "img.png").exists() or (fail / "img.png").exists():
                break
            time.sleep(0.2)
        assert (proc / "img.png").exists() or (fail / "img.png").exists()
    finally:
        runner.stop()
        queue.stop_worker()
