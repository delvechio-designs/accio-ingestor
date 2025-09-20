# accio-ingestor/src/accio_ingestor/main.py
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config import settings, ensure_dirs
from .logging_cfg import init_logging
from .watcher import WatchRunner
from .pdf_utils import process_file_to_payload
from .queue import JobQueue
from .jobs import enqueue_ingest_jobs
from .slack import SlackClient


def main() -> int:
    parser = argparse.ArgumentParser(prog="accio-ingestor", description="Accio Ingestor CLI")
    parser.add_argument("--gui", action="store_true", help="Launch GUI")
    parser.add_argument("--watch", action="store_true", help="Run headless watcher")
    parser.add_argument("--oneshot", action="store_true", help="Process a single file then exit")
    parser.add_argument("--input", type=str, help="Input file path for --oneshot")
    args = parser.parse_args()

    init_logging()
    ensure_dirs()

    if args.gui:
        from .gui import launch_gui
        return launch_gui()

    if args.watch:
        queue = JobQueue()
        queue.start_worker()
        runner = WatchRunner(queue=queue)
        try:
            runner.start_blocking()
        except KeyboardInterrupt:
            pass
        finally:
            runner.stop()
            queue.stop_worker()
        return 0

    if args.oneshot:
        if not args.input:
            print("--oneshot requires --input <path>", file=sys.stderr)
            return 2
        in_path = Path(args.input).expanduser().resolve()
        if not in_path.exists():
            print(f"Input not found: {in_path}", file=sys.stderr)
            return 3
        queue = JobQueue()
        queue.start_worker()
        try:
            payload, original_bytes, content_type, sha256 = process_file_to_payload(in_path)
            enqueue_ingest_jobs(queue, payload, sha256, original_bytes, content_type, in_path.name)
            # Move to processed
            processed = Path(settings.PROCESSED_DIR)
            processed.mkdir(parents=True, exist_ok=True)
            (processed / in_path.name).write_bytes(original_bytes)
        finally:
            queue.stop_worker()
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
