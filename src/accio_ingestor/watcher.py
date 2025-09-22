# accio-ingestor/src/accio_ingestor/watcher.py
from __future__ import annotations

import shutil
import time
from pathlib import Path
from typing import Optional

from watchdog.events import FileSystemEventHandler, FileCreatedEvent
from watchdog.observers import Observer

from .config import settings, ensure_dirs
from .logging_cfg import get_logger
from .pdf_utils import process_file_to_payload
from .queue import JobQueue
from .jobs import enqueue_ingest_jobs
from .slack import SlackClient

log = get_logger(__name__)


def _is_stable(path: Path, wait_ms: int = 500) -> bool:
    """Two identical size/mtime samples >= wait_ms apart."""
    stat1 = path.stat()
    time.sleep(wait_ms / 1000.0)
    stat2 = path.stat()
    return stat1.st_size == stat2.st_size and stat1.st_mtime == stat2.st_mtime


class Handler(FileSystemEventHandler):
    def __init__(self, queue: JobQueue):
        self.queue = queue
        self.slack = SlackClient()

    def on_created(self, event):
        if isinstance(event, FileCreatedEvent):
            path = Path(event.src_path)
            suffix = path.suffix.lower()
            if suffix not in {".pdf", ".jpg", ".jpeg", ".png"}:
                return
            try:
                log.info(f"new file detected: {path.name}")
                # Wait until stable
                for _ in range(60):  # up to ~30 sec
                    if _is_stable(path):
                        break
                    time.sleep(0.5)
                else:
                    raise TimeoutError("file not stable")

                payload, original_bytes, content_type, sha256 = process_file_to_payload(path)

                if self.queue.is_seen(sha256):
                    log.info(f"duplicate skipped: {path.name} sha256={sha256}")
                    # Move to processed anyway to avoid re-triggering
                    dest = Path(settings.PROCESSED_DIR) / path.name
                    dest.write_bytes(original_bytes)
                    path.unlink(missing_ok=True)
                    return

                self.queue.mark_seen(sha256, path.name)
                enqueue_ingest_jobs(self.queue, payload, sha256, original_bytes, content_type, path.name)

                # Move to processed
                dest = Path(settings.PROCESSED_DIR) / path.name
                shutil.move(str(path), str(dest))
                log.info(f"moved to processed: {dest}")
            except Exception as e:
                log.error(f"processing failed for {path.name}: {e}")
                self.slack.error("PROCESSING_FAILED", {"filename": path.name, "message": str(e)})
                # move to failed
                try:
                    dest = Path(settings.FAILED_DIR) / path.name
                    shutil.move(str(path), str(dest))
                except Exception:
                    pass


class WatchRunner:
    def __init__(self, queue: JobQueue):
        self.queue = queue
        self._observer = Observer()
        self._stop = False

    def start_blocking(self) -> None:
        ensure_dirs()
        handler = Handler(self.queue)
        self._observer.schedule(handler, settings.WATCH_DIR, recursive=False)
        self._observer.start()
        log.info("watcher started")
        try:
            while not self._stop:
                time.sleep(0.5)
        finally:
            self._observer.stop()
            self._observer.join()
            log.info("watcher stopped")

    def stop(self) -> None:
        self._stop = True
