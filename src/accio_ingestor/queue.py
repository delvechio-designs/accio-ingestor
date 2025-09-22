# accio-ingestor/src/accio_ingestor/queue.py
from __future__ import annotations

import base64
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

from .config import settings
from .logging_cfg import get_logger
from .pii import redact

log = get_logger(__name__)

_DB_PATH = Path(".").joinpath("queue.db").resolve()


class JobQueue:
    def __init__(self, db_path: Path = _DB_PATH) -> None:
        self.db_path = db_path
        self.lock = threading.Lock()
        self._stop = threading.Event()
        self._worker: Optional[threading.Thread] = None
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._conn() as con:
            cur = con.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    sha256 TEXT,
                    attempts INTEGER DEFAULT 0,
                    next_at INTEGER DEFAULT 0,
                    created_at INTEGER DEFAULT 0
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS seen_files (
                    sha256 TEXT PRIMARY KEY,
                    filename TEXT,
                    created_at INTEGER
                )
                """
            )
            con.commit()

    def mark_seen(self, sha256: str, filename: str) -> None:
        with self._conn() as con:
            con.execute(
                "INSERT OR IGNORE INTO seen_files(sha256, filename, created_at) VALUES (?,?,?)",
                (sha256, filename, int(time.time())),
            )

    def is_seen(self, sha256: str) -> bool:
        with self._conn() as con:
            row = con.execute("SELECT 1 FROM seen_files WHERE sha256=?", (sha256,)).fetchone()
            return row is not None

    def enqueue(self, type_: str, payload: Dict[str, Any], sha256: Optional[str] = None) -> int:
        with self._conn() as con:
            cur = con.cursor()
            cur.execute(
                "INSERT INTO jobs(type, payload, sha256, attempts, next_at, created_at) VALUES (?,?,?,?,?,?)",
                (type_, json.dumps(payload), sha256, 0, int(time.time()), int(time.time())),
            )
            job_id = cur.lastrowid
            con.commit()
            log.info(f"enqueued job id={job_id} type={type_}")
            return job_id

    def _dequeue_due(self) -> Optional[sqlite3.Row]:
        with self._conn() as con:
            cur = con.cursor()
            row = cur.execute(
                "SELECT * FROM jobs WHERE next_at <= ? ORDER BY id LIMIT 1", (int(time.time()),)
            ).fetchone()
            if row:
                # lock row by updating next_at into the future briefly to avoid concurrent workers
                cur.execute("UPDATE jobs SET next_at=? WHERE id=?", (int(time.time()) + 5, row["id"]))
                con.commit()
            return row

    def start_worker(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        self._stop.clear()
        self._worker = threading.Thread(target=self._run, daemon=True)
        self._worker.start()

    def stop_worker(self) -> None:
        self._stop.set()
        if self._worker:
            self._worker.join(timeout=5)

    def count_jobs(self) -> int:
        with self._conn() as con:
            row = con.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()
            return int(row["c"] if row else 0)

    def _run(self) -> None:
        from .jobs import handle_job  # local import to avoid cycles

        log.info("job worker started")
        while not self._stop.is_set():
            row = self._dequeue_due()
            if not row:
                time.sleep(0.25)
                continue

            job_id = int(row["id"])
            type_ = row["type"]
            attempts = int(row["attempts"])
            payload = json.loads(row["payload"])

            try:
                handle_job(type_, payload)
            except Exception as e:
                attempts += 1
                backoff = min(60, 2 ** attempts)  # 2s → 60s
                next_at = int(time.time()) + backoff
                with self._conn() as con:
                    con.execute(
                        "UPDATE jobs SET attempts=?, next_at=? WHERE id=?",
                        (attempts, next_at, job_id),
                    )
                    con.commit()
                if attempts >= 10:
                    log.error(f"job {job_id} FAILED after {attempts} attempts: {redact(str(e))}")
                    # final failure: notify Slack via jobs.handle_job
                else:
                    log.warning(f"job {job_id} retry in {backoff}s due to {redact(str(e))}")
            else:
                with self._conn() as con:
                    con.execute("DELETE FROM jobs WHERE id=?", (job_id,))
                    con.commit()
                log.info(f"job {job_id} done")
        log.info("job worker stopped")
